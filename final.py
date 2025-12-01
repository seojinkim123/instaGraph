import requests
import time
from collections import deque
from neo4j import GraphDatabase

########################################################
# Instagram 설정
########################################################

INSTAGRAM_UA = (
    "Instagram 280.0.0.20.113 Android "
    "(30/11; 420dpi; 1080x1920; Samsung; SM-G973N; beyond1; exynos9820; en_US; 465869920)"
)

########################################################
# Neo4j 연결
########################################################

driver = GraphDatabase.driver(
    "bolt://localhost:7687",
    auth=("neo4j", "10041004")  # ← 네 비밀번호 넣기!
)

def save_user(tx, user_id, username, full_name, is_verified):
    tx.run("""
        MERGE (u:User {id: $id})
        SET u.username = $username,
            u.full_name = $full_name,
            u.is_verified = $is_verified
    """, id=user_id, username=username, full_name=full_name, is_verified=is_verified)

def save_follow_relation(tx, src_id, dst_id):
    tx.run("""
        MATCH (a:User {id: $src_id}), (b:User {id: $dst_id})
        MERGE (a)-[:FOLLOWS]->(b)
    """, src_id=src_id, dst_id=dst_id)


########################################################
# username → user_id
########################################################

def username_to_user_id(username, cookie):
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
    except:
        print("❌ JSON 파싱 실패:", r.text[:200])
        return None


########################################################
# user_id → following 목록 전체 가져오기
########################################################

def get_following(user_id, cookie):
    url = f"https://i.instagram.com/api/v1/friendships/{user_id}/following/"

    params = {"count": 50}
    headers = {
        "User-Agent": INSTAGRAM_UA,
        "Cookie": cookie,
    }

    all_users = []

    while True:
        r = requests.get(url, params=params, headers=headers)

        print("[Following 응답]", r.status_code)

        if r.status_code != 200:
            print("❌ following 조회 실패:", r.text[:200])
            return all_users

        try:
            data = r.json()
        except:
            print("❌ JSON 파싱 실패:", r.text[:200])
            return all_users

        if "users" not in data:
            break

        all_users.extend(data["users"])

        # 페이징 처리
        if "next_max_id" in data:
            params["max_id"] = data["next_max_id"]
            time.sleep(1)  # rate limit 방지
        else:
            break

    return all_users


########################################################
# BFS 크롤러 (단일 스레드 — 안정성 최강)
########################################################

def bfs_crawl(start_username, cookie, depth_limit=2):

    start_id = username_to_user_id(start_username, cookie)
    if not start_id:
        print("❌ user_id 조회 실패")
        return

    queue = deque()
    queue.append((start_id, 0))

    visited = set()
    visited.add(start_id)

    print(f"🚀 BFS 시작: {start_username} (user_id={start_id})")

    with driver.session() as session_db:

        # 시작 유저 저장
        session_db.execute_write(save_user, start_id, start_username, "", False)

        while queue:
            user_id, depth = queue.popleft()

            if depth > depth_limit:
                continue

            print(f"\n🔍 depth={depth}, 크롤링 user_id={user_id}")

            followings = get_following(user_id, cookie)
            print(f"▶ following 수 = {len(followings)}")

            for u in followings:
                dst_id = u["id"]
                username = u.get("username", "")
                full_name = u.get("full_name", "")
                is_verified = u.get("is_verified", False)

                # Neo4j 사용자 저장
                session_db.execute_write(save_user, dst_id, username, full_name, is_verified)
                # Neo4j 관계 저장
                session_db.execute_write(save_follow_relation, user_id, dst_id)

                # BFS 확장
                if dst_id not in visited:
                    visited.add(dst_id)
                    queue.append((dst_id, depth + 1))

    return visited


########################################################
# 실행
########################################################

cookie = 'csrftoken=U7FXjmOe_MgvHktwe5vZmn; datr=cbyqaMBs2ooJlzayVIfv-l8z; ig_did=3C93A83B-CA2A-4DBC-BB3F-E8DCD6A63D9C; ig_nrcb=1; mid=aKq8dgAEAAFWaaPYD7nVuqXRzWkm; ps_l=1; ps_n=1; ds_user_id=4223704197; ig_lang=ko; sessionid=4223704197%3Ac4Jnq1ijqpanQm%3A22%3AAYgDoQ3Wjq8Am90Rz-fjvM39XffOSp4nuSV4naUBIA; rur="VLL\0544223704197\0541796016861:01fed97d48627db54da4ff59c82727b98689bd37aaa563d21a7e0982dd5cba586f55c39c"'

visited_users = bfs_crawl("katarinabluu", cookie, depth_limit=1)

print("\n🎉 BFS 크롤링 완료!")
print("총 방문한 user 수:", len(visited_users))