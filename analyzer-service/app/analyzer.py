# C:\Users\user\Desktop\TechStats\analyzer-service\app\analyzer.py
import re
import asyncio
from typing import Dict, List, Set, Tuple, Optional, Any
from collections import defaultdict
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import SnowballStemmer
import pymorphy3
import structlog

from config import settings
from app.tech_patterns import TechPatternsLoader

logger = structlog.get_logger()


class TextAnalyzer:
    """Анализатор текста для поиска технологий"""
    
    def __init__(self):
        self.morph_analyzer: Optional[pymorphy3.MorphAnalyzer] = None
        self.stemmer: Optional[SnowballStemmer] = None
        self.stopwords: Set[str] = set()
        self.nltk_downloaded = False
        
    async def initialize(self):
        """Инициализация NLP инструментов"""
        await self._download_nltk_data()
        
        # Инициализация pymorphy3 для русского языка
        if settings.language == "ru":
            self.morph_analyzer = pymorphy3.MorphAnalyzer()
            self.stemmer = SnowballStemmer("russian")
        else:
            self.morph_analyzer = None
            self.stemmer = SnowballStemmer("english")
        
        # Загрузка стоп-слов
        if settings.remove_stopwords:
            try:
                if settings.language == "ru":
                    self.stopwords = set(stopwords.words('russian'))
                else:
                    self.stopwords = set(stopwords.words('english'))
            except:
                # Fallback стоп-слова
                self.stopwords = {
                    'и', 'в', 'во', 'не', 'что', 'он', 'на', 'я', 'с', 'со', 'как',
                    'а', 'то', 'все', 'она', 'так', 'его', 'но', 'да', 'ты', 'к',
                    'у', 'же', 'вы', 'за', 'бы', 'по', 'только', 'ее', 'мне', 'было',
                    'вот', 'от', 'меня', 'еще', 'нет', 'о', 'из', 'ему', 'теперь',
                    'когда', 'даже', 'ну', 'вдруг', 'ли', 'если', 'уже', 'или', 'ни',
                    'быть', 'был', 'него', 'до', 'вас', 'нибудь', 'опять', 'уж', 'вам',
                    'ведь', 'там', 'потом', 'себя', 'ничего', 'ей', 'может', 'они',
                    'тут', 'где', 'есть', 'надо', 'ней', 'для', 'мы', 'тебя', 'их',
                    'чем', 'была', 'сам', 'чтоб', 'без', 'будто', 'чего', 'раз', 'тоже',
                    'себе', 'под', 'будет', 'ж', 'тогда', 'кто', 'этот', 'того', 'потому',
                    'этого', 'какой', 'совсем', 'ним', 'здесь', 'этом', 'один', 'почти',
                    'мой', 'тем', 'чтобы', 'нее', 'сейчас', 'были', 'куда', 'зачем',
                    'всех', 'никогда', 'можно', 'при', 'наконец', 'два', 'об', 'другой',
                    'хоть', 'после', 'над', 'больше', 'тот', 'через', 'эти', 'нас',
                    'про', 'всего', 'них', 'какая', 'много', 'разве', 'три', 'эту', 'моя',
                    'впрочем', 'хорошо', 'свою', 'этой', 'перед', 'иногда', 'лучше',
                    'чуть', 'том', 'нельзя', 'такой', 'им', 'более', 'всегда', 'конечно',
                    'всю', 'между'
                }
        
        logger.info("Text analyzer initialized", language=settings.language)
    
    async def _download_nltk_data(self):
        """Загрузка необходимых данных NLTK"""
        if self.nltk_downloaded:
            return
        
        try:
            # Проверяем, установлены ли данные
            nltk.data.find('tokenizers/punkt')
            nltk.data.find('corpora/stopwords')
        except LookupError:
            # Загружаем в фоновом режиме
            await asyncio.to_thread(self._download_nltk_sync)
        
        self.nltk_downloaded = True
    
    def _download_nltk_sync(self):
        """Синхронная загрузка данных NLTK"""
        nltk.download('punkt', quiet=True)
        nltk.download('stopwords', quiet=True)
    
    def preprocess_text(self, text: str) -> str:
        """Предобработка текста"""
        if not text:
            return ""
        
        # Приведение к нижнему регистру
        text = text.lower()
        
        # Удаление HTML тегов
        text = re.sub(r'<[^>]+>', ' ', text)
        
        # Удаление URL
        text = re.sub(r'https?://\S+|www\.\S+', ' ', text)
        
        # Удаление email
        text = re.sub(r'\S+@\S+', ' ', text)
        
        # Удаление специальных символов, оставляем буквы, цифры и пробелы
        text = re.sub(r'[^\w\s]', ' ', text)
        
        # Замена множественных пробелов на один
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def tokenize_text(self, text: str) -> List[str]:
        """Токенизация текста"""
        if not text:
            return []
        
        try:
            tokens = word_tokenize(text, language=settings.language)
        except:
            # Fallback токенизация
            tokens = text.split()
        
        return tokens
    
    def normalize_token(self, token: str) -> str:
        """Нормализация токена"""
        if settings.enable_lemmatization and self.morph_analyzer:
            # Лемматизация для русского языка
            parsed = self.morph_analyzer.parse(token)
            if parsed:
                return parsed[0].normal_form
        
        if settings.enable_stemming and self.stemmer:
            # Стемминг
            return self.stemmer.stem(token)
        
        return token
    
    def process_text(self, text: str) -> List[str]:
        """Полная обработка текста"""
        # Предобработка
        cleaned_text = self.preprocess_text(text)
        
        # Токенизация
        tokens = self.tokenize_text(cleaned_text)
        
        # Нормализация и фильтрация
        processed_tokens = []
        for token in tokens:
            # Пропускаем стоп-слова
            if settings.remove_stopwords and token in self.stopwords:
                continue
            
            # Нормализация
            normalized = self.normalize_token(token)
            if normalized:
                processed_tokens.append(normalized)
        
        return processed_tokens
    
    def extract_keywords(self, text: str, max_keywords: int = 20) -> List[Tuple[str, float]]:
        """Извлечение ключевых слов из текста"""
        tokens = self.process_text(text)
        
        # Подсчет частоты
        freq_dist = defaultdict(int)
        for token in tokens:
            freq_dist[token] += 1
        
        # Нормализация частот
        total_tokens = len(tokens)
        if total_tokens == 0:
            return []
        
        keyword_scores = [
            (word, count / total_tokens)
            for word, count in freq_dist.items()
        ]
        
        # Сортировка по частоте
        keyword_scores.sort(key=lambda x: x[1], reverse=True)
        
        return keyword_scores[:max_keywords]
    
    def find_ngrams(self, text: str, n: int = 2) -> List[Tuple[str, ...]]:
        """Поиск N-грамм в тексте"""
        tokens = self.process_text(text)
        
        if len(tokens) < n:
            return []
        
        ngrams = []
        for i in range(len(tokens) - n + 1):
            ngram = tuple(tokens[i:i+n])
            ngrams.append(ngram)
        
        return ngrams


class PatternMatcher:
    """Поиск технологий по паттернам"""
    
    def __init__(self, text_analyzer: TextAnalyzer, patterns_loader: TechPatternsLoader):
        self.text_analyzer = text_analyzer
        self.patterns_loader = patterns_loader
        self.cache = {}
        
    async def find_technology(
        self,
        text: str,
        technology: str,
        search_fields: List[str] = None
    ) -> Dict[str, Any]:
        """
        Поиск конкретной технологии в тексте
        
        Args:
            text: Текст для анализа
            technology: Название технологии для поиска
            search_fields: Где искать (title, snippet, description)
            
        Returns:
            Результат поиска
        """
        if search_fields is None:
            search_fields = ["title", "snippet", "description"]
        
        # Получение паттерна технологии
        pattern_data = self.patterns_loader.get_pattern(technology)
        if not pattern_data:
            # Если паттерна нет, создаем простой regex
            pattern = re.compile(r'\b' + re.escape(technology.lower()) + r'\b', re.IGNORECASE)
            pattern_name = technology
            category = "unknown"
        else:
            pattern = self.patterns_loader.get_compiled_pattern(technology)
            pattern_name = pattern_data.get("name", technology)
            category = pattern_data.get("category", "unknown")
        
        if not pattern:
            return {
                "found": False,
                "technology": technology,
                "pattern_name": pattern_name,
                "matches": [],
                "search_fields": search_fields
            }
        
        # Поиск совпадений
        matches = []
        found = False
        
        # Разбиваем текст на секции если указаны поля
        if isinstance(text, dict):
            # Текст в формате {"title": "...", "snippet": "...", "description": "..."}
            text_dict = text
        else:
            # Простой текст
            text_dict = {"full_text": text}
        
        for field, field_text in text_dict.items():
            if field not in search_fields and field != "full_text":
                continue
            
            if not field_text:
                continue
            
            # Поиск по простому regex
            regex_matches = list(pattern.finditer(field_text.lower()))
            if regex_matches:
                found = True
                for match in regex_matches:
                    matches.append({
                        "field": field,
                        "text": match.group(),
                        "start": match.start(),
                        "end": match.end(),
                        "context": self._get_context(field_text, match.start(), match.end())
                    })
        
        # Дополнительный поиск по нормализованным токенам
        if not found and settings.enable_stemming:
            processed_text = ' '.join(self.text_analyzer.process_text(str(text)))
            if processed_text:
                regex_matches = list(pattern.finditer(processed_text))
                if regex_matches:
                    found = True
                    for match in regex_matches:
                        matches.append({
                            "field": "normalized",
                            "text": match.group(),
                            "start": match.start(),
                            "end": match.end(),
                            "context": self._get_context(processed_text, match.start(), match.end())
                        })
        
        return {
            "found": found,
            "technology": technology,
            "pattern_name": pattern_name,
            "category": category,
            "matches": matches,
            "match_count": len(matches),
            "search_fields": search_fields
        }
    
    async def find_multiple_technologies(
        self,
        text: str,
        technologies: List[str],
        search_fields: List[str] = None
    ) -> Dict[str, Any]:
        """Поиск нескольких технологий в тексте"""
        if search_fields is None:
            search_fields = ["title", "snippet", "description"]
        
        results = {}
        found_technologies = []
        
        # Параллельный поиск технологий
        tasks = [
            self.find_technology(text, tech, search_fields)
            for tech in technologies
        ]
        
        tech_results = await asyncio.gather(*tasks)
        
        for result in tech_results:
            tech_id = result["technology"]
            results[tech_id] = result
            if result["found"]:
                found_technologies.append(tech_id)
        
        return {
            "total_technologies": len(technologies),
            "found_technologies": len(found_technologies),
            "found_percentage": (len(found_technologies) / len(technologies) * 100) if technologies else 0,
            "technologies": found_technologies,
            "results": results,
            "search_fields": search_fields
        }
    
    async def analyze_vacancy(
        self,
        vacancy_data: Dict[str, Any],
        technology: str,
        exact_search: bool = True
    ) -> Dict[str, Any]:
        """Анализ вакансии на наличие технологии"""
        vacancy_id = vacancy_data.get("id", "")
        vacancy_name = vacancy_data.get("name", "")
        
        # Извлечение текстовых полей
        text_to_analyze = {
            "title": vacancy_name,
            "snippet": "",
            "description": vacancy_data.get("description", "")
        }
        
        # Добавляем сниппет если есть
        snippet = vacancy_data.get("snippet", {})
        if snippet:
            requirement = snippet.get("requirement", "")
            responsibility = snippet.get("responsibility", "")
            text_to_analyze["snippet"] = f"{requirement} {responsibility}"
        
        # Определяем порядок поиска
        search_fields = ["title", "snippet", "description"]
        
        # Если точный поиск, добавляем кавычки
        search_technology = technology
        if exact_search:
            search_technology = f'"{technology}"'
        
        # Поиск технологии
        result = await self.find_technology(
            text_to_analyze,
            search_technology,
            search_fields
        )
        
        # Если не найдено с точным поиском, пробуем без кавычек
        if not result["found"] and exact_search:
            result = await self.find_technology(
                text_to_analyze,
                technology,
                search_fields
            )
        
        return {
            "vacancy_id": vacancy_id,
            "vacancy_name": vacancy_name,
            "vacancy_url": vacancy_data.get("alternate_url", ""),
            "analysis_result": result,
            "has_technology": result["found"],
            "match_count": result["match_count"],
            "search_type": "exact" if exact_search else "fuzzy"
        }
    
    async def analyze_vacancies_batch(
        self,
        vacancies: List[Dict[str, Any]],
        technology: str,
        exact_search: bool = True,
        batch_size: int = 10
    ) -> List[Dict[str, Any]]:
        """Пакетный анализ вакансий"""
        results = []
        
        # Разбиваем на батчи
        for i in range(0, len(vacancies), batch_size):
            batch = vacancies[i:i + batch_size]
            
            # Параллельный анализ батча
            batch_tasks = [
                self.analyze_vacancy(vacancy, technology, exact_search)
                for vacancy in batch
            ]
            
            batch_results = await asyncio.gather(*batch_tasks)
            results.extend(batch_results)
            
            logger.debug(
                "Batch analyzed",
                batch=i // batch_size + 1,
                total_batches=(len(vacancies) + batch_size - 1) // batch_size,
                processed=min(i + batch_size, len(vacancies)),
                total=len(vacancies)
            )
        
        return results
    
    async def analyze_vacancies_with_progress(
        self,
        vacancies: List[Dict[str, Any]],
        technology: str,
        exact_search: bool = True,
        progress_callback=None
    ) -> Dict[str, Any]:
        """Анализ вакансий с отслеживанием прогресса"""
        total_vacancies = len(vacancies)
        if total_vacancies == 0:
            return {
                "total_vacancies": 0,
                "tech_vacancies": 0,
                "tech_percentage": 0,
                "vacancies_with_tech": [],
                "analysis_details": []
            }
        
        vacancies_with_tech = []
        analysis_details = []
        processed = 0
        
        for vacancy in vacancies:
            # Анализ вакансии
            result = await self.analyze_vacancy(vacancy, technology, exact_search)
            
            if result["has_technology"]:
                vacancies_with_tech.append({
                    "id": result["vacancy_id"],
                    "name": result["vacancy_name"],
                    "url": result["vacancy_url"],
                    "match_count": result["match_count"]
                })
            
            analysis_details.append(result)
            processed += 1
            
            # Отправка прогресса
            if progress_callback and processed % 10 == 0:
                progress = (processed / total_vacancies) * 100
                await progress_callback(
                    stage="analyzing",
                    message=f"Обработано вакансий: {processed}/{total_vacancies}",
                    progress=progress,
                    processed=processed,
                    total=total_vacancies,
                    found_with_tech=len(vacancies_with_tech)
                )
        
        tech_vacancies = len(vacancies_with_tech)
        tech_percentage = (tech_vacancies / total_vacancies * 100) if total_vacancies > 0 else 0
        
        return {
            "total_vacancies": total_vacancies,
            "tech_vacancies": tech_vacancies,
            "tech_percentage": round(tech_percentage, 2),
            "vacancies_with_tech": vacancies_with_tech,
            "analysis_details": analysis_details
        }
    
    def _get_context(self, text: str, start: int, end: int, context_size: int = 50) -> str:
        """Получение контекста вокруг совпадения"""
        context_start = max(0, start - context_size)
        context_end = min(len(text), end + context_size)
        
        context = text[context_start:context_end]
        
        # Добавляем многоточия если обрезали
        if context_start > 0:
            context = "..." + context
        if context_end < len(text):
            context = context + "..."
        
        return context