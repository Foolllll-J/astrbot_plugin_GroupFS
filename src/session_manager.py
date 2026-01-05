import time
from typing import List, Dict, Optional

class SearchSession:
    def __init__(self, keyword: str, results: List[Dict], page_size: int = 10):
        self.keyword = keyword
        self.results = results
        self.total_count = len(results)
        self.current_page = 1
        self.page_size = page_size
        self.timestamp = time.time()

    def get_page_results(self, page: int) -> List[Dict]:
        """获取指定页的结果。"""
        start = (page - 1) * self.page_size
        end = start + self.page_size
        return self.results[start:end]

    def is_expired(self, timeout: int) -> bool:
        """检查会话是否过期。"""
        return time.time() - self.timestamp > timeout

    def refresh(self):
        """刷新会话活跃时间。"""
        self.timestamp = time.time()

class SessionManager:
    def __init__(self, timeout: int = 300):
        self.sessions: Dict[str, SearchSession] = {}  # key: f"{group_id}_{user_id}"
        self.timeout = timeout

    def create_session(self, group_id: int, user_id: int, keyword: str, results: List[Dict], page_size: int = 10) -> SearchSession:
        key = f"{group_id}_{user_id}"
        session = SearchSession(keyword, results, page_size)
        self.sessions[key] = session
        return session

    def get_session(self, group_id: int, user_id: int) -> Optional[SearchSession]:
        key = f"{group_id}_{user_id}"
        session = self.sessions.get(key)
        if session:
            if session.is_expired(self.timeout):
                del self.sessions[key]
                return None
            session.refresh()
            return session
        return None

    def clear_session(self, group_id: int, user_id: int):
        key = f"{group_id}_{user_id}"
        if key in self.sessions:
            del self.sessions[key]

    def cleanup_expired(self):
        """清理所有过期的会话。"""
        now = time.time()
        expired_keys = [k for k, s in self.sessions.items() if now - s.timestamp > self.timeout]
        for k in expired_keys:
            del self.sessions[k]
