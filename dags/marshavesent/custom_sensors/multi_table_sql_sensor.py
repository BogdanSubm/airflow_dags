from airflow.sensors.base import BaseSensorOperator
from airflow.hooks.base import BaseHook
from airflow.utils.decorators import apply_defaults
from typing import List, Dict, Optional, Union, Any
import psycopg2 as pg


class MultiTableSQLSensor(BaseSensorOperator):
    """
    Кастомный сенсор для проверки наличия данных в нескольких таблицах PostgreSQL.
    edf 
    Args:
        tables: Список названий таблиц для проверки
        postgres_conn_id: ID подключения к PostgreSQL
        database: Название базы данных
        min_rows: Минимальное количество строк в каждой таблице
        check_all: Проверять все таблицы или хотя бы одну
    """
    
    template_fields = ('tables',)
    ui_color = '#4ea5d9'
    
    @apply_defaults
    def __init__(
        self,
        tables: List[str],
        postgres_conn_id: str = 'conn_pg',
        database: str = 'etl',
        min_rows: int = 1,
        check_all: bool = True,  # True - все таблицы должны иметь данные
        *args, **kwargs
    ) -> None:
        # По умолчанию mode='reschedule' для экономии ресурсов
        kwargs.setdefault('mode', 'reschedule')
        kwargs.setdefault('poke_interval', 300)  # Проверка каждые 5 минут
        kwargs.setdefault('timeout', 3600)  # Таймаут 1 час
        
        super().__init__(*args, **kwargs)
        self.tables = tables
        self.postgres_conn_id = postgres_conn_id
        self.database = database
        self.min_rows = min_rows
        self.check_all = check_all
    
    def _get_connection(self):
        """
        Создает подключение к PostgreSQL.

        """
        connection = BaseHook.get_connection(self.postgres_conn_id)
        return pg.connect(
            dbname=self.database,
            sslmode='disable',
            user=connection.login,
            password=connection.password,
            host=connection.host,
            port=connection.port,
            connect_timeout=10
        )
    
    def _check_table(self, cursor, table_name: str) -> Dict[str, Any]:
        """
        Проверяет наличие данных в конкретной таблице.
        
        Возвращает словарь с результатами проверки.
        """
        try:
            # Проверяем существование таблицы
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                )
            """, (table_name,))
            
            table_exists = cursor.fetchone()[0]
            
            if not table_exists:
                return {
                    'table': table_name,
                    'exists': False,
                    'has_data': False,
                    'row_count': 0,
                    'error': f'Таблица {table_name} не существует'
                }
            
            # Считаем количество строк
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
            
            return {
                'table': table_name,
                'exists': True,
                'has_data': row_count >= self.min_rows,
                'row_count': row_count
            }
            
        except Exception as e:
            return {
                'table': table_name,
                'exists': False,
                'has_data': False,
                'row_count': 0,
                'error': str(e)
            }
    
    def poke(self, context: Dict) -> bool:
        """
        Основной метод проверки (вызывается каждые poke_interval секунд).
        
        Args:
            context
            
        Returns:
            bool: True если условие выполнено, False если нужно продолжить ожидание
        """
        execution_date = context.get('execution_date')
        self.log.info(f'Проверка данных за {execution_date}')
        self.log.info(f'Таблицы для проверки: {self.tables}')
        self.log.info(f'Режим: {"все таблицы" if self.check_all else "хотя бы одна"}')
        
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            results = []
            ready_tables = []
            not_ready_tables = []
            
            # Проверяем каждую таблицу
            for table in self.tables:
                result = self._check_table(cursor, table)
                results.append(result)
                
                if result.get('has_data'):
                    ready_tables.append(table)
                else:
                    not_ready_tables.append(table)
            
            # Логируем результаты
            self.log.info(f'Готовые таблицы: {ready_tables}')
            if not_ready_tables:
                self.log.warning(f'Не готовые таблицы: {not_ready_tables}')
                for result in results:
                    if not result.get('has_data'):
                        error_msg = result.get('error', 
                            f'Недостаточно данных: {result["row_count"]} строк (нужно {self.min_rows})')
                        self.log.warning(f"  {result['table']}: {error_msg}")
            
            # Определяем успешность проверки
            if self.check_all:
                # Все таблицы должны быть готовы
                is_ready = len(not_ready_tables) == 0
            else:
                # Хотя бы одна таблица готова
                is_ready = len(ready_tables) > 0
            
            if is_ready:
                self.log.info('✅ Все проверки пройдены! Продолжаем выполнение.')
                return True
            else:
                self.log.info('⏳ Данные еще не готовы. Ожидаем...')
                return False
                
        except Exception as e:
            self.log.error(f'Ошибка при проверке: {e}')
            # При ошибке возвращаем False, чтобы сенсор продолжил попытки
            return False
        finally:
            if conn:
                conn.close()