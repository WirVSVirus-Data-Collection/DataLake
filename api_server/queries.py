from typing import Dict, List


class QueryFactory:
    def __init__(self, categories: Dict[str, List[str]]):
        self.categories = categories
