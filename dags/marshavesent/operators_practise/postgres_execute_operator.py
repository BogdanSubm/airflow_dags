from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.utils.decorators import apply_defaults
from typing import Dict, List, Any, Optional
import psycopg2 as pg
from psycopg2.extras import RealDictCursor
import importlib

class PostgresExecuteOperator(BaseOperator):
    """
    Кастомный оператор для выполнения SQL запросов к PostgreSQL.
    Поддерживает Jinja шаблоны через template_fields.
    """
    
    template_fields = ('sql', 'parameters')
    template_ext = ('.sql',)
    ui_color = '#3580c4'
    
    @apply_defaults
    def __init__(
        self,
        sql: str,
        postgres_conn_id: str = 'conn_pg',
        database: str = 'etl',
        parameters: Optional[Dict] = None,
        autocommit: bool = True,
        show_result: bool = False,
        *args, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        self.database = database
        self.parameters = parameters or {}
        self.autocommit = autocommit
        self.show_result = show_result
    
    def execute(self, context):
        """Выполняет SQL запрос к PostgreSQL."""
        self.log.info(f'Выполнение SQL в базе {self.database}')
        
        # Логируем запрос (первые 300 символов)
        sql_preview = self.sql[:300].replace('\n', ' ').strip()
        self.log.info(f'SQL: {sql_preview}...')
        
        if self.parameters:
            self.log.info(f'Параметры: {self.parameters}')
        
        connection = BaseHook.get_connection(self.postgres_conn_id)
        
        try:
            with pg.connect(
                dbname=self.database,
                sslmode='disable',
                user=connection.login,
                password=connection.password,
                host=connection.host,
                port=connection.port,
                connect_timeout=600,
                keepalives_idle=600,
                tcp_user_timeout=600
            ) as conn:
                
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(self.sql, self.parameters)
                    
                    if self.show_result and cur.description:
                        rows = cur.fetchall()
                        self.log.info(f'Результат запроса: {len(rows)} строк')
                        for i, row in enumerate(rows[:5]):
                            self.log.info(f'  Строка {i+1}: {dict(row)}')
                    
                    if self.autocommit:
                        conn.commit()
                        if cur.rowcount >= 0:
                            self.log.info(f'Затронуто строк: {cur.rowcount}')
                    
                    self.log.info('Запрос выполнен успешно')
                    
        except Exception as e:
            self.log.error(f'Ошибка SQL: {str(e)}')
            raise


class PostgresBranchOperator(BaseOperator):
    """
    Гибкий BranchOperator для проверки дня месяца.
    Поддерживает загрузку дней из:
    - Прямого параметра allowed_days
    - Python модуля (config_module)
    - JSON файла (config_file)
    """
    
    template_fields = ('allowed_days',)
    ui_color = '#ff7f50'
    
    @apply_defaults
    def __init__(
        self,
        allowed_days: List[int] = None,
        config_module: str = None,
        config_file: str = None,
        task_id_to_continue: str = None,
        task_id_to_skip: str = None,
        *args, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.allowed_days = allowed_days
        self.config_module = config_module
        self.config_file = config_file
        self.task_id_to_continue = task_id_to_continue
        self.task_id_to_skip = task_id_to_skip
    
    def _get_allowed_days(self) -> List[int]:
        """Определяет итоговый список разрешенных дней."""
        days = None
        
        # Приоритет 1: Прямой параметр
        if self.allowed_days:
            days = self.allowed_days
        
        # Приоритет 2: Python модуль
        if self.config_module:
            try:
                module = importlib.import_module(self.config_module)
                days = getattr(module, 'MONTHLY_RUN_DAYS', None) or getattr(module, 'ALLOWED_DAYS', None)
                if days:
                    self.log.info(f'Дни из модуля {self.config_module}: {days}')
            except ImportError:
                self.log.warning(f'Не удалось импортировать {self.config_module}')
        
        # Приоритет 3: JSON файл
        if self.config_file:
            import json, os
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r') as f:
                    config = json.load(f)
                    days = config.get('allowed_days', [])
                self.log.info(f'Дни из JSON: {days}')
        
        # По умолчанию
        if not days:
            days = [1, 2, 5]
            self.log.info(f'Дни по умолчанию: {days}')
        
        return days
    
    def execute(self, context):
        """Проверяет день и возвращает ID следующей задачи."""
        execution_date = context['execution_date']
        current_day = execution_date.day
        allowed_days = self._get_allowed_days()
        
        self.log.info(f'{"=" * 40}')
        self.log.info(f'Дата: {execution_date.date()}')
        self.log.info(f'День месяца: {current_day}')
        self.log.info(f'Разрешенные дни: {allowed_days}')
        
        if current_day in allowed_days:
            self.log.info(f'✓ День {current_day} разрешен для выполнения')
            return self.task_id_to_continue
        else:
            self.log.info(f'✗ День {current_day} пропущен')
            return self.task_id_to_skip