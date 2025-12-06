# C:\Users\user\Desktop\TechStats\analyzer-service\app\tech_patterns.py
import json
import re
from typing import Dict, List, Set, Any, Optional
from pathlib import Path
import asyncio
import structlog

from config import settings
from app.cache import cache_manager

logger = structlog.get_logger()


class TechPatternsLoader:
    """Загрузчик и менеджер паттернов технологий"""
    
    def __init__(self):
        self.patterns: Dict[str, Dict[str, Any]] = {}
        self.compiled_patterns: Dict[str, re.Pattern] = {}
        self.categories: Set[str] = set()
        self.aliases: Dict[str, str] = {}  # alias -> main_technology
        
    async def load_patterns(self):
        """Загрузка паттернов из файла и кэша"""
        cache_key = "tech_patterns:compiled"
        
        # Попытка загрузки из кэша
        cached = await cache_manager.get(cache_key)
        if cached:
            self.patterns = cached.get("patterns", {})
            self.categories = set(cached.get("categories", []))
            self.aliases = cached.get("aliases", {})
            self._compile_patterns()
            logger.info("Patterns loaded from cache", count=len(self.patterns))
            return
        
        # Загрузка из файла
        patterns_file = Path(settings.tech_patterns_file)
        
        if not patterns_file.exists():
            logger.warning("Patterns file not found, creating default", file=str(patterns_file))
            await self._create_default_patterns()
            await self._save_patterns_to_file()
        else:
            await self._load_patterns_from_file(patterns_file)
        
        # Кэширование паттернов
        await self._cache_patterns()
    
    async def _load_patterns_from_file(self, patterns_file: Path):
        """Загрузка паттернов из JSON файла"""
        try:
            with open(patterns_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            self.patterns = data.get("patterns", {})
            self.categories = set(data.get("categories", []))
            self.aliases = data.get("aliases", {})
            
            self._compile_patterns()
            logger.info("Patterns loaded from file", count=len(self.patterns))
            
        except Exception as e:
            logger.error("Failed to load patterns from file", error=str(e), file=str(patterns_file))
            await self._create_default_patterns()
    
    async def _create_default_patterns(self):
        """Создание паттернов по умолчанию"""
        self.patterns = {
            "python": {
                "name": "Python",
                "category": "programming_language",
                "patterns": [
                    r'\bpython\b',
                    r'\bpython3\b',
                    r'\bpython3\.\d+\b',
                    r'\bcpython\b',
                    r'\bdjango\b',
                    r'\bflask\b',
                    r'\bfastapi\b',
                    r'\bnumpy\b',
                    r'\bpandas\b',
                    r'\bscikit-learn\b',
                    r'\btensorflow\b',
                    r'\bpytorch\b'
                ],
                "weight": 1.0,
                "aliases": ["py", "python3", "python3.11", "django", "flask"],
                "description": "Язык программирования Python и его фреймворки"
            },
            "java": {
                "name": "Java",
                "category": "programming_language",
                "patterns": [
                    r'\bjava\b',
                    r'\bjava\s*\d+\b',
                    r'\bspring\b',
                    r'\bhibernate\b',
                    r'\bmaven\b',
                    r'\bgradle\b',
                    r'\bjunit\b',
                    r'\bmockito\b'
                ],
                "weight": 1.0,
                "aliases": ["java8", "java11", "java17", "spring", "hibernate"],
                "description": "Язык программирования Java и его экосистема"
            },
            "javascript": {
                "name": "JavaScript",
                "category": "programming_language",
                "patterns": [
                    r'\bjavascript\b',
                    r'\bjs\b',
                    r'\bnode\.?js\b',
                    r'\bnodejs\b',
                    r'\btypescript\b',
                    r'\bts\b',
                    r'\breact\b',
                    r'\bangular\b',
                    r'\bvue\b',
                    r'\bexpress\b',
                    r'\bnestjs\b'
                ],
                "weight": 1.0,
                "aliases": ["js", "node", "nodejs", "ts", "typescript", "react", "angular", "vue"],
                "description": "JavaScript и его фреймворки"
            },
            "sql": {
                "name": "SQL",
                "category": "database",
                "patterns": [
                    r'\bsql\b',
                    r'\bpostgresql\b',
                    r'\bmysql\b',
                    r'\bmariadb\b',
                    r'\bsqlite\b',
                    r'\bmssql\b',
                    r'\bsql\s*server\b',
                    r'\boracle\b',
                    r'\bpl/sql\b'
                ],
                "weight": 1.0,
                "aliases": ["postgres", "postgresql", "mysql", "mariadb"],
                "description": "Язык SQL и реляционные СУБД"
            },
            "docker": {
                "name": "Docker",
                "category": "devops",
                "patterns": [
                    r'\bdocker\b',
                    r'\bdockerfile\b',
                    r'\bdocker-compose\b',
                    r'\bcontainer\b',
                    r'\bcontainers\b'
                ],
                "weight": 1.0,
                "aliases": ["docker-compose", "container"],
                "description": "Контейнеризация с Docker"
            },
            "kubernetes": {
                "name": "Kubernetes",
                "category": "devops",
                "patterns": [
                    r'\bkubernetes\b',
                    r'\bk8s\b',
                    r'\bhelm\b',
                    r'\bkubectl\b',
                    r'\bminikube\b'
                ],
                "weight": 1.0,
                "aliases": ["k8s", "helm"],
                "description": "Оркестрация контейнеров с Kubernetes"
            },
            "aws": {
                "name": "AWS",
                "category": "cloud",
                "patterns": [
                    r'\baws\b',
                    r'\bamazon\s*web\s*services\b',
                    r'\bec2\b',
                    r'\bs3\b',
                    r'\blambda\b',
                    r'\brds\b',
                    r'\bcloudformation\b',
                    r'\becs\b',
                    r'\beck\b'
                ],
                "weight": 1.0,
                "aliases": ["amazon", "ec2", "s3", "lambda"],
                "description": "Amazon Web Services"
            },
            "linux": {
                "name": "Linux",
                "category": "os",
                "patterns": [
                    r'\blinux\b',
                    r'\bubuntu\b',
                    r'\bcentos\b',
                    r'\bdebian\b',
                    r'\bredhat\b',
                    r'\bfedora\b',
                    r'\bunix\b'
                ],
                "weight": 1.0,
                "aliases": ["ubuntu", "centos", "debian"],
                "description": "Операционные системы на базе Linux"
            }
        }
        
        self.categories = {
            "programming_language",
            "database", 
            "devops",
            "cloud",
            "os",
            "framework",
            "library",
            "tool"
        }
        
        self._build_aliases()
        self._compile_patterns()
        
        logger.info("Default patterns created", count=len(self.patterns))
    
    def _build_aliases(self):
        """Построение словаря алиасов"""
        self.aliases = {}
        for tech_id, tech_data in self.patterns.items():
            for alias in tech_data.get("aliases", []):
                self.aliases[alias.lower()] = tech_id
    
    def _compile_patterns(self):
        """Компиляция regex паттернов"""
        for tech_id, tech_data in self.patterns.items():
            patterns = tech_data.get("patterns", [])
            if patterns:
                # Объединяем все паттерны через | (OR)
                combined_pattern = '|'.join(f'({p})' for p in patterns)
                try:
                    self.compiled_patterns[tech_id] = re.compile(
                        combined_pattern,
                        re.IGNORECASE | re.UNICODE
                    )
                except re.error as e:
                    logger.error("Failed to compile pattern", tech_id=tech_id, error=str(e))
    
    async def _save_patterns_to_file(self):
        """Сохранение паттернов в файл"""
        patterns_file = Path(settings.tech_patterns_file)
        patterns_file.parent.mkdir(parents=True, exist_ok=True)
        
        data = {
            "patterns": self.patterns,
            "categories": list(self.categories),
            "aliases": self.aliases,
            "metadata": {
                "version": settings.version,
                "loaded_at": asyncio.get_event_loop().time(),
                "count": len(self.patterns)
            }
        }
        
        try:
            with open(patterns_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info("Patterns saved to file", file=str(patterns_file))
        except Exception as e:
            logger.error("Failed to save patterns to file", error=str(e))
    
    async def _cache_patterns(self):
        """Кэширование скомпилированных паттернов"""
        cache_key = "tech_patterns:compiled"
        data = {
            "patterns": self.patterns,
            "categories": list(self.categories),
            "aliases": self.aliases
        }
        
        await cache_manager.set(
            cache_key,
            data,
            ttl=settings.pattern_cache_ttl_hours * 3600
        )
    
    def get_pattern(self, technology: str) -> Optional[Dict[str, Any]]:
        """Получение паттерна по названию технологии"""
        # Поиск по прямому совпадению
        if technology.lower() in self.patterns:
            return self.patterns[technology.lower()]
        
        # Поиск по алиасу
        normalized = technology.lower()
        if normalized in self.aliases:
            tech_id = self.aliases[normalized]
            return self.patterns.get(tech_id)
        
        # Поиск по частичному совпадению
        for tech_id, tech_data in self.patterns.items():
            if normalized in tech_id or any(normalized in alias.lower() for alias in tech_data.get("aliases", [])):
                return tech_data
        
        return None
    
    def get_compiled_pattern(self, technology: str) -> Optional[re.Pattern]:
        """Получение скомпилированного паттерна"""
        pattern_data = self.get_pattern(technology)
        if pattern_data:
            tech_id = self._get_tech_id(technology)
            return self.compiled_patterns.get(tech_id)
        return None
    
    def _get_tech_id(self, technology: str) -> Optional[str]:
        """Получение ID технологии"""
        normalized = technology.lower()
        
        if normalized in self.patterns:
            return normalized
        
        if normalized in self.aliases:
            return self.aliases[normalized]
        
        for tech_id in self.patterns:
            if normalized == tech_id.lower():
                return tech_id
        
        return None
    
    def get_all_patterns(self) -> Dict[str, Dict[str, Any]]:
        """Получение всех паттернов"""
        return self.patterns.copy()
    
    def get_categories(self) -> List[str]:
        """Получение всех категорий"""
        return list(self.categories)
    
    def get_technologies_by_category(self, category: str) -> List[Dict[str, Any]]:
        """Получение технологий по категории"""
        return [
            {**tech_data, "id": tech_id}
            for tech_id, tech_data in self.patterns.items()
            if tech_data.get("category") == category
        ]
    
    def add_pattern(
        self,
        tech_id: str,
        name: str,
        patterns: List[str],
        category: str = "other",
        aliases: Optional[List[str]] = None,
        weight: float = 1.0,
        description: str = ""
    ) -> bool:
        """Добавление нового паттерна"""
        tech_id = tech_id.lower()
        
        if tech_id in self.patterns:
            logger.warning("Pattern already exists", tech_id=tech_id)
            return False
        
        self.patterns[tech_id] = {
            "name": name,
            "category": category,
            "patterns": patterns,
            "weight": weight,
            "aliases": aliases or [],
            "description": description
        }
        
        self.categories.add(category)
        
        if aliases:
            for alias in aliases:
                self.aliases[alias.lower()] = tech_id
        
        # Перекомпиляция паттерна
        combined_pattern = '|'.join(f'({p})' for p in patterns)
        try:
            self.compiled_patterns[tech_id] = re.compile(
                combined_pattern,
                re.IGNORECASE | re.UNICODE
            )
        except re.error as e:
            logger.error("Failed to compile new pattern", tech_id=tech_id, error=str(e))
            del self.patterns[tech_id]
            return False
        
        logger.info("Pattern added", tech_id=tech_id, name=name)
        return True
    
    def remove_pattern(self, tech_id: str) -> bool:
        """Удаление паттерна"""
        tech_id = tech_id.lower()
        
        if tech_id not in self.patterns:
            return False
        
        # Удаление алиасов
        tech_data = self.patterns[tech_id]
        for alias in tech_data.get("aliases", []):
            if alias.lower() in self.aliases:
                del self.aliases[alias.lower()]
        
        # Удаление паттерна
        del self.patterns[tech_id]
        del self.compiled_patterns[tech_id]
        
        logger.info("Pattern removed", tech_id=tech_id)
        return True
    
    async def save_and_cache(self):
        """Сохранение паттернов в файл и кэш"""
        await self._save_patterns_to_file()
        await self._cache_patterns()