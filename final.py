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
INSTAGRAM_COOKIE = '여기에_네_인스타그램_쿠키_문자열'

# 큐에서 한 번에 가져올 작업 개수 (너무 작으면 DB 왕복 많아지고, 너무 크면 메모리 잡아먹음)
TASK_BATCH_SIZE = 10

# RUNNING 상태 작업이 이 시간(ms) 이상 지났으면 PENDING으로 되돌림 (이전 실행이 죽었다고 판단)
STALE_RUNNING_MS = 10 * 60 * 1000  # 10분


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

def username_to_user_id(username: str, cookie: str) -> str | None:
    url = f"https://i.instagram.com/api/v1/users/web_profile_info/?username={username}"

    headers = {
        "User-Agent": INSTAGRAM_UA,
        "Cookie": cookie,
    }

    r = requests.get(url, headers=headers)
    print("[UserID 응답]", r.status_code)

    if r.status_code != 200:
        print("❌ user_id 조회 실패:", r.text[:200])
        return None

    try:
        data = r.json()
        return data["data"]["user"]["id"]
    except Exception as e:
        print("❌ JSON 파싱 실패:", e, r.text[:200])
        return None


def get_following(user_id: str, cookie: str) -> list[dict]:
    """
    user_id 기준으로 팔로잉 전체 목록 가져오기.
    인스타가 막거나 에러나면 적당히 그 시점까지 수집한 것만 반환.
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

def save_start_user_and_task(tx, user_id: str, username: str, depth: int):
    """
    시작 유저 저장 + 시작 큐 작업 생성 (이미 있으면 무시)
    """
    tx.run("""
    MERGE (u:User {id: $id})
      ON CREATE SET u.username = $username,
                    u.full_name = "",
                    u.is_verified = false
      ON MATCH SET  u.username = coalesce(u.username, $username)
    
    MERGE (t:CrawlTask {user_id: $id, depth: $depth})
      ON CREATE SET
        t.status = 'PENDING',
        t.try_count = 0,
        t.created_at = timestamp(),
        t.updated_at = timestamp()
    """, id=user_id, username=username, depth=depth)


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
    ORDER BY t.created_at
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
    한 유저의 팔로잉 전체를 한 번에 배치로 저장:
      - User 노드 upsert
      - (src)-[:FOLLOWS]->(dst) 관계 생성
      - depth+1 에 대한 CrawlTask 생성 (큐 삽입)
    """
    if not followings:
        return

    next_depth = depth + 1

    tx.run("""
    WITH $followings AS followings, $src_id AS src_id,
         $next_depth AS next_depth, $depth_limit AS depth_limit

    // 1) User upsert
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

def bfs_crawl_persistent(start_username: str, cookie: str, depth_limit: int = 2):
    """
    - 시작 username을 user_id로 변환
    - Neo4j 안에 큐(:CrawlTask)를 만들어서 BFS
    - 프로세스가 죽어도 DB에 남은 큐를 기준으로 재시작 가능
    """
    start_id = username_to_user_id(start_username, cookie)
    if not start_id:
        print("❌ user_id 조회 실패")
        return

    with driver.session() as session:
        # 인덱스/제약조건 생성
        init_db()

        # 이전 실행에서 죽은 RUNNING 작업들 복구
        session.execute_write(reset_stale_running_tasks, STALE_RUNNING_MS)

        # 시작 유저 + 시작 작업 enqueue (이미 있으면 무시됨)
        session.execute_write(save_start_user_and_task, start_id, start_username, 0)

        print(f"🚀 BFS 시작: {start_username} (user_id={start_id}), depth_limit={depth_limit}")

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
                    followings = get_following(user_id, cookie)
                    print(f"▶ following 수 = {len(followings)}")

                    # 팔로잉 정보 + 큐 삽입을 한 번의 트랜잭션으로 처리
                    session.execute_write(
                        store_followings_and_enqueue,
                        user_id, depth, depth_limit, followings
                    )

                    # 작업 완료 처리
                    session.execute_write(mark_task_done, user_id, depth)
                    processed_count += 1

                except Exception as e:
                    print("❌ get_following 처리 중 오류:", e)
                    # 에러난 작업 마킹 (나중에 따로 재시도 할 수도 있음)
                    session.execute_write(
                        mark_task_error, user_id, depth, str(e)
                    )
                    # 너무 공격적으로 재시도하면 차단 위험 → 여기서는 그냥 다음 작업으로 넘어감
                    continue

        print("\n🎉 BFS 크롤링 완료!")
        print("총 처리한 작업 수:", processed_count)


########################################################
# 실행 예시
########################################################

if __name__ == "__main__":
    # 실제로 사용할 때 username / cookie / depth_limit 설정해서 호출
    bfs_crawl_persistent(
        start_username="katarinabluu",
        cookie=INSTAGRAM_COOKIE,
        depth_limit=1
    )