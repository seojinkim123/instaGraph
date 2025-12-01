import requests
import time
from collections import namedtuple
from neo4j import GraphDatabase

########################################################
# 설정
########################################################

INSTAGRAM_UA = (
    "Instagram 280.0.0.20.113 Android "
    "(30/11; 420dpi; 1080x1920; Samsung; SM-G973N; beyond1; exynos9820; en_US; 465869920)"
)

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "10041004"   # 네 비밀번호

# 네 인스타그램 쿠키 문자열 넣기
INSTAGRAM_COOKIE = 'csrftoken=U7FXjmOe_MgvHktwe5vZmn; datr=cbyqaMBs2ooJlzayVIfv-l8z; ig_did=3C93A83B-CA2A-4DBC-BB3F-E8DCD6A63D9C; ig_nrcb=1; mid=aKq8dgAEAAFWaaPYD7nVuqXRzWkm; ps_l=1; ps_n=1; ds_user_id=4223704197; ig_lang=ko; sessionid=4223704197%3Ac4Jnq1ijqpanQm%3A22%3AAYgDoQ3Wjq8Am90Rz-fjvM39XffOSp4nuSV4naUBIA; rur="VLL\0544223704197\0541796016861:01fed97d48627db54da4ff59c82727b98689bd37aaa563d21a7e0982dd5cba586f55c39c"'

# 큐에서 한 번에 가져올 작업 개수 (너무 작으면 DB 왕복 많아지고, 너무 크면 메모리 잡아먹음)
TASK_BATCH_SIZE = 10

# RUNNING 상태 작업이 이 시간(ms) 이상 지났으면 PENDING으로 되돌림 (이전 실행이 죽었다고 판단)
STALE_RUNNING_MS = 10 * 60 * 1000  # 10분

# 팔로워 수 기준 (50000 이상인 계정만 확장)
MIN_FOLLOWERS = 50000


########################################################
# Neo4j 드라이버
########################################################

driver = GraphDatabase.driver(
    NEO4J_URI,
    auth=(NEO4J_USER, NEO4J_PASSWORD)
)

Task = namedtuple("Task", ["user_id", "depth"])


########################################################
# DB 초기화 (인덱스 / 제약조건)
########################################################

def init_db():
    """
    User, CrawlTask에 대한 인덱스/제약조건 생성.
    최초 1번만 실행되지만, IF NOT EXISTS라서 여러 번 실행해도 괜찮음.
    """
    with driver.session() as session:
        session.run("""
        CREATE CONSTRAINT user_id_unique IF NOT EXISTS
        FOR (u:User)
        REQUIRE u.id IS UNIQUE
        """)
        # Neo4j 5 기준 복합 제약조건
        session.run("""
        CREATE CONSTRAINT crawltask_unique IF NOT EXISTS
        FOR (t:CrawlTask)
        REQUIRE (t.user_id, t.depth) IS UNIQUE
        """)


########################################################
# Instagram API
########################################################

def username_to_user_id(username: str, cookie: str) -> dict | None:
    """
    username을 web_profile_info API로 조회해서
      - id
      - username
      - full_name
      - followers (팔로워 수)
    를 함께 반환.

    기존에는 id만 반환했는데, followers까지 같이 준다.
    """
    url = f"https://i.instagram.com/api/v1/users/web_profile_info/?username={username}"

    headers = {
        "User-Agent": INSTAGRAM_UA,
        "Cookie": cookie,
    }

    r = requests.get(url, headers=headers)
    print(f"[UserProfile 응답] {username} ->", r.status_code)

    if r.status_code != 200:
        print("❌ user profile 조회 실패:", r.text[:200])
        return None

    try:
        data = r.json()
        user = data["data"]["user"]

        # edge_followed_by: { count: <팔로워 수> }
        followers = None
        edge_followed_by = user.get("edge_followed_by")
        if isinstance(edge_followed_by, dict):
            followers = edge_followed_by.get("count")

        return {
            "id": user.get("id"),
            "username": user.get("username"),
            "full_name": user.get("full_name"),
            "followers": followers,
        }
    except Exception as e:
        print("❌ JSON 파싱 실패:", e, r.text[:200])
        return None


def get_following(user_id: str, cookie: str) -> list[dict]:
    """
    user_id 기준으로 '팔로우하고 있는 계정들' 전체 목록 가져오기.
    인스타가 막거나 에러나면 적당히 그 시점까지 수집한 것만 반환.
    (여기서는 팔로워 수 정보는 안 온다고 가정)
    """
    url = f"https://i.instagram.com/api/v1/friendships/{user_id}/following/"

    params = {"count": 50}
    headers = {
        "User-Agent": INSTAGRAM_UA,
        "Cookie": cookie,
    }

    all_users: list[dict] = []

    while True:
        r = requests.get(url, params=params, headers=headers)
        print("[Following 응답]", r.status_code)

        if r.status_code != 200:
            # 429나 5xx 등도 여기로 들어옴
            print("❌ following 조회 실패:", r.text[:200])
            return all_users

        try:
            data = r.json()
        except Exception as e:
            print("❌ JSON 파싱 실패:", e, r.text[:200])
            return all_users

        if "users" not in data:
            break

        all_users.extend(data["users"])

        # 페이징 처리
        next_max_id = data.get("next_max_id")
        if next_max_id:
            params["max_id"] = next_max_id
            # rate limit 방지
            time.sleep(1)
        else:
            break

    return all_users


########################################################
# Neo4j: User / 관계 / 큐(CrawlTask) 관련 쿼리
########################################################

def save_start_user_and_task(tx, user_id: str, username: str, depth: int, follower_count: int | None):
    """
    시작 유저 저장 + 시작 큐 작업 생성 (이미 있으면 무시)
    follower_count가 있으면 User 노드에 같이 저장.
    """
    tx.run("""
    MERGE (u:User {id: $id})
      ON CREATE SET u.username = $username,
                    u.full_name = "",
                    u.is_verified = false,
                    u.follower_count = $follower_count
      ON MATCH SET  u.username = coalesce(u.username, $username),
                    u.follower_count = coalesce(u.follower_count, $follower_count)

    MERGE (t:CrawlTask {user_id: $id, depth: $depth})
      ON CREATE SET
        t.status = 'PENDING',
        t.try_count = 0,
        t.created_at = timestamp(),
        t.updated_at = timestamp()
    """, id=user_id, username=username, depth=depth, follower_count=follower_count)


def reset_stale_running_tasks(tx, stale_ms: int):
    """
    이전 실행에서 RUNNING 상태로 죽어버린 작업들을 PENDING으로 되돌리기.
    """
    tx.run("""
    MATCH (t:CrawlTask)
    WHERE t.status = 'RUNNING'
      AND t.updated_at < timestamp() - $stale_ms
    SET t.status = 'PENDING'
    """, stale_ms=stale_ms)


def fetch_next_tasks(tx, depth_limit: int, batch_size: int) -> list[Task]:
    """
    PENDING 상태에서 depth_limit 이하인 작업 몇 개를 가져와서 RUNNING으로 바꾸고 반환.
    """
    result = tx.run("""
    MATCH (t:CrawlTask)
    WHERE t.status = 'PENDING'
      AND t.depth <= $depth_limit
    WITH t
    ORDER BY t.depth ASC, t.created_at ASC
    LIMIT $batch_size
    SET t.status = 'RUNNING',
        t.updated_at = timestamp()
    RETURN t.user_id AS user_id, t.depth AS depth
    """, depth_limit=depth_limit, batch_size=batch_size)

    rows = result.data()
    return [Task(row["user_id"], row["depth"]) for row in rows]


def mark_task_done(tx, user_id: str, depth: int):
    tx.run("""
    MATCH (t:CrawlTask {user_id: $user_id, depth: $depth})
    SET t.status = 'DONE',
        t.updated_at = timestamp()
    """, user_id=user_id, depth=depth)


def mark_task_error(tx, user_id: str, depth: int, error_msg: str):
    tx.run("""
    MATCH (t:CrawlTask {user_id: $user_id, depth: $depth})
    SET t.status = 'ERROR',
        t.try_count = coalesce(t.try_count, 0) + 1,
        t.last_error = $error_msg,
        t.updated_at = timestamp()
    """, user_id=user_id, depth=depth, error_msg=error_msg[:500])


def store_followings_and_enqueue(tx, src_id: str, depth: int, depth_limit: int, followings: list[dict]):
    """
    한 유저의 '팔로우하고 있는 계정들'(followings)을 한 번에 배치로 저장:
      - User 노드 upsert (팔로워 수는 아직 모름 → 나중에 task에서 채움)
      - (src)-[:FOLLOWS]->(dst) 관계 생성
      - depth+1 CrawlTask 큐에 삽입
    """
    if not followings:
        print("⚠ Neo4j에 저장할 followings 없음")
        return

    next_depth = depth + 1

    tx.run("""
    WITH $followings AS followings, $src_id AS src_id,
         $next_depth AS next_depth, $depth_limit AS depth_limit

    // 1) User upsert (팔로잉 계정들)
    UNWIND followings AS f
    MERGE (dst:User {id: f.id})
    SET dst.username   = f.username,
        dst.full_name  = f.full_name,
        dst.is_verified = f.is_verified

    // 2) FOLLOWS 관계
    WITH collect(dst) AS dsts, src_id, next_depth, depth_limit
    MATCH (src:User {id: src_id})
    UNWIND dsts AS dst
    MERGE (src)-[:FOLLOWS]->(dst)

    // 3) 큐(CrawlTask) 삽입 (다음 depth, depth_limit 이하인 경우만)
    WITH dsts, next_depth, depth_limit
    WHERE next_depth <= depth_limit
    UNWIND dsts AS dst2
    MERGE (t:CrawlTask {user_id: dst2.id, depth: next_depth})
      ON CREATE SET
        t.status = 'PENDING',
        t.try_count = 0,
        t.created_at = timestamp(),
        t.updated_at = timestamp()
    """, followings=[
        {
            "id": u["id"],
            "username": u.get("username", ""),
            "full_name": u.get("full_name", ""),
            "is_verified": u.get("is_verified", False),
        }
        for u in followings
    ], src_id=src_id, next_depth=next_depth, depth_limit=depth_limit)


########################################################
# BFS 크롤링 (Persistent Queue 버전)
########################################################

def bfs_crawl_persistent(start_usernames, cookie: str, depth_limit: int = 2):
    """
    - 시작 username(들)을 user_profile API로 변환 (id + followers 등)
    - Neo4j 안에 큐(:CrawlTask)를 여러 개 만들어서 BFS 시작점 여러 개 등록
    - 큐에서 꺼낼 때 followers 수를 처음 1번만 조회하고,
      MIN_FOLLOWERS 미만이면 거기서 확장 중단
    - 프로세스가 죽어도 DB에 남은 큐를 기준으로 재시작 가능

    start_usernames:
      - 문자열 하나 ("katarinabluu")
      - 또는 문자열 리스트(["a", "b", "c"])
    """
    # 문자열 하나 들어와도 리스트로 변환
    if isinstance(start_usernames, str):
        start_usernames = [start_usernames]

    # 혹시 빈 리스트가 들어오면 바로 종료
    if not start_usernames:
        print("❌ start_usernames 가 비어 있습니다.")
        return

    with driver.session() as session:
        # 인덱스/제약조건 생성
        init_db()

        # 이전 실행에서 죽은 RUNNING 작업들 복구
        session.execute_write(reset_stale_running_tasks, STALE_RUNNING_MS)

        # 여러 시작 유저 처리
        start_infos: list[tuple[str, str, int | None]] = []  # (username, user_id, followers)

        for username in start_usernames:
            info = username_to_user_id(username, cookie)
            if not info or not info.get("id"):
                print(f"❌ {username} → user_id 조회 실패, 이 유저는 스킵.")
                continue

            start_id = info["id"]
            followers = info.get("followers")
            print(f"⭐ start user: {username} (id={start_id}, followers={followers})")

            # 시작 유저 + 시작 작업 enqueue (이미 있으면 MERGE라 중복 X)
            session.execute_write(
                save_start_user_and_task,
                start_id,
                username,
                0,
                followers
            )
            start_infos.append((username, start_id, followers))

        if not start_infos:
            print("❌ 시작 가능한 유저가 하나도 없습니다. 종료.")
            return

        print(f"🚀 BFS 시작 (start points {len(start_infos)}개, depth_limit={depth_limit}, MIN_FOLLOWERS={MIN_FOLLOWERS})")
        for uname, uid, foll in start_infos:
            print(f"   - {uname} (user_id={uid}, followers={foll})")

        processed_count = 0

        while True:
            # 1) 큐에서 작업 묶음 가져오기
            tasks: list[Task] = session.execute_write(
                fetch_next_tasks, depth_limit, TASK_BATCH_SIZE
            )

            if not tasks:
                print("✅ 더 이상 처리할 작업이 없습니다. 종료.")
                break

            print(f"\n📦 이번 배치 작업 수: {len(tasks)}")

            for task in tasks:
                user_id = task.user_id
                depth = task.depth

                print(f"\n🔍 depth={depth}, 크롤링 user_id={user_id}")

                try:
                    # 1) 이 user_id에 대한 username / follower_count 를 Neo4j에서 읽기
                    rec = session.run("""
                        MATCH (u:User {id: $id})
                        RETURN u.username AS username, u.follower_count AS follower_count
                    """, id=user_id).single()

                    if rec is None or rec["username"] is None:
                        print("⚠ username 없는 User, 스킵")
                        session.execute_write(mark_task_done, user_id, depth)
                        continue

                    username = rec["username"]
                    follower_count = rec["follower_count"]

                    # 2) follower_count가 아직 없으면 (처음 보는 유저면) API 한 번만 호출
                    if follower_count is None:
                        info = username_to_user_id(username, cookie)
                        if not info or info.get("followers") is None:
                            print("⚠ 팔로워 수 조회 실패, 이 유저는 확장 안 함")
                            session.execute_write(mark_task_done, user_id, depth)
                            continue

                        follower_count = info["followers"]

                        # DB에 한 번 저장해두고, 다음부터는 API 안 부르게 함
                        session.run("""
                            MATCH (u:User {id: $id})
                            SET u.follower_count = $followers
                        """, id=user_id, followers=follower_count)

                    print(f"👥 {username} followers = {follower_count}")

                    # 3) 기준보다 작으면 그냥 여기서 끝 (확장 안 함)
                    if follower_count < MIN_FOLLOWERS:
                        print(f"🚫 팔로워 {MIN_FOLLOWERS} 미만, 팔로잉 확장 스킵")
                        session.execute_write(mark_task_done, user_id, depth)
                        continue

                    # 4) 여기까지 왔으면 MIN_FOLLOWERS 이상 → 실제 BFS 확장
                    followings = get_following(user_id, cookie)
                    print(f"▶ following 수 = {len(followings)}")

                    session.execute_write(
                        store_followings_and_enqueue,
                        user_id, depth, depth_limit, followings
                    )

                    # 작업 완료 처리
                    session.execute_write(mark_task_done, user_id, depth)
                    processed_count += 1

                except Exception as e:
                    print("❌ get_following / 확장 처리 중 오류:", e)
                    # 에러난 작업 마킹 (나중에 따로 재시도 할 수도 있음)
                    session.execute_write(
                        mark_task_error, user_id, depth, str(e)
                    )
                    continue  # 다음 task 로

        print("\n🎉 BFS 크롤링 완료!")
        print("총 처리한 작업 수:", processed_count)


########################################################
# 실행 예시
########################################################

if __name__ == "__main__":
    # 1) 하나만 넣고 싶으면 문자열
    # bfs_crawl_persistent(
    #     start_usernames="katarinabluu",
    #     cookie=INSTAGRAM_COOKIE,
    #     depth_limit=1
    # )

    # 2) 여러 개를 동시에 시작점으로 주고 싶으면 리스트
    bfs_crawl_persistent(
        start_usernames=["smtown", "bts.bighitofficial", "for_everyoung10"],
        cookie=INSTAGRAM_COOKIE,
        depth_limit=2
    )