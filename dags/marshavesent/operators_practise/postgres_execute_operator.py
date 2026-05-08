from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.utils.decorators import apply_defaults
from typing import Dict, List, Any, Optional
import psycopg2 as pg
from psycopg2.extras import RealDictCursor
import jinja2

class PostgresExecuteOperator(BaseOperator):
    """
    Кастомный оператор для выполнения SQL запросов к PostgreSQL.
    Поддерживает Jinja шаблоны в SQL запросах и параметрах.
    Не возвращает данные (за исключением SELECT запросов при необходимости отладки).
    
    Attributes:
        sql: SQL запрос или путь к файлу с запросом
        postgres_conn_id: ID подключения к PostgreSQL
        parameters: Параметры для SQL запроса
        autocommit: Автоматический коммит после выполнения
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
        show_result: bool = False,  # Для отладки
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
        """
        Выполняет SQL запрос.
        """
        self.log.info(f'Выполнение SQL запроса...')
        
        # Рендерим Jinja шаблоны
        sql_to_execute = self.sql
        if self.parameters:
            sql_to_execute = self.render_template(sql_to_execute, context)
        
        self.log.info(f'SQL: {sql_to_execute[:200]}...')
        self.log.info(f'Параметры: {self.parameters}')
        
        # Получаем подключение
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
                    # Выполняем запрос
                    cur.execute(sql_to_execute, self.parameters)
                    
                    # Показываем результат если нужно (для отладки SELECT)
                    if self.show_result and cur.description:
                        rows = cur.fetchall()
                        self.log.info(f'Результат ({len(rows)} строк):')
                        for row in rows[:5]:  # Показываем только первые 5 строк
                            self.log.info(f'  {row}')
                    
                    # Коммит
                    if self.autocommit:
                        conn.commit()
                        self.log.info('Запрос выполнен успешно')
                    
        except Exception as e:
            self.log.error(f'Ошибка выполнения SQL: {e}')
            raise e
    
    def render_template(self, sql: str, context: Dict) -> str:
        """
        Рендерит Jinja шаблоны в SQL запросе.
        """
        template = jinja2.Template(sql)
        rendered_sql = template.render(**context)
        return rendered_sql


class PostgresBranchOperator(BaseOperator):
    """
    Кастомный BranchOperator для проверки дня месяца.
    Позволяет выполнять задачи только в определенные дни месяца.
    """
    
    template_fields = ('allowed_days',)
    ui_color = '#ff7f50'
    
    @apply_defaults
    def __init__(
        self,
        allowed_days: List[int] = None,
        task_id_to_continue: str = None,
        task_id_to_skip: str = None,
        *args, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.allowed_days = allowed_days or [1, 2, 5]  # По умолчанию 1, 2, 5 числа
        self.task_id_to_continue = task_id_to_continue
        self.task_id_to_skip = task_id_to_skip
    
    def execute(self, context):
        """
        Проверяет день месяца и возвращает соответствующую задачу.
        """
        execution_date = context['execution_date']
        current_day = execution_date.day
        
        self.log.info(f'Текущий день месяца: {current_day}')
        self.log.info(f'Разрешенные дни: {self.allowed_days}')
        
        if current_day in self.allowed_days:
            self.log.info(f'День {current_day} разрешен, продолжаем выполнение')
            if self.task_id_to_continue:
                return self.task_id_to_continue
            return None
        else:
            self.log.info(f'День {current_day} пропущен')
            if self.task_id_to_skip:
                return self.task_id_to_skip
            return None