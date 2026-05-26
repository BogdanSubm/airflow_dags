from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State
from typing import List, Dict
from datetime import timedelta


class MultiExternalTaskSensor(ExternalTaskSensor):
    """
    Кастомный сенсор для проверки нескольких External Tasks.
    
    ## Документация Airflow:
    
    ExternalTaskSensor:
    - Проверяет статус задачи в ДРУГОМ DAG
    - Использует execution_date для сопоставления
    - Можно указать execution_delta для проверки задач с другой датой
    
    Важные параметры родительского класса:
    - external_dag_id: ID внешнего DAG
    - external_task_id: ID внешней задачи (или None для проверки всего DAG)
    - execution_delta: разница во времени между DAG
    - allowed_states: список состояний, которые считаются успешными
    
    В реализации:
    - Поддерживаем список из нескольких DAG/Task комбинаций
    - Можно указать разные allowed_states для разных проверок
    - Все проверки выполняются в одном poke() методе
    """
    
    template_fields = ('external_tasks',)
    ui_color = '#ff8c42'
    
    @apply_defaults
    def __init__(
        self,
        external_tasks: List[Dict[str, str]] = None,
        *args, **kwargs
    ) -> None:
        """
        Args:
            external_tasks: Список словарей с информацией о внешних задачах.
                Каждый словарь должен содержать:
                - external_dag_id: ID внешнего DAG
                - external_task_id: ID внешней задачи
                Может содержать:
                - execution_delta: timedelta для сдвига даты
                - allowed_states: список разрешенных состояний
                
                Пример:
                [
                    {
                        'external_dag_id': 'dag1',
                        'external_task_id': 'task1',
                        'allowed_states': ['success']
                    },
                    {
                        'external_dag_id': 'dag2',
                        'external_task_id': 'task2',
                        'execution_delta': timedelta(hours=1)
                    }
                ]
        """

        kwargs.setdefault('mode', 'reschedule')
        kwargs.setdefault('poke_interval', 300)
        kwargs.setdefault('timeout', 7200)  # 2 часа
        
        super().__init__(*args, **kwargs)
        self.external_tasks = external_tasks or []
        
        # Валидация входных данных
        if not self.external_tasks:
            raise ValueError("external_tasks не может быть пустым")
        
        for i, task_config in enumerate(self.external_tasks):
            if 'external_dag_id' not in task_config:
                raise ValueError(f"В задаче {i} отсутствует external_dag_id")
            if 'external_task_id' not in task_config:
                raise ValueError(f"В задаче {i} отсутствует external_task_id")
    
    def _check_single_task(self, context: Dict, task_config: Dict) -> Dict:
        """
        Проверяет статус одной внешней задачи.
        
        Использует механизм родительского ExternalTaskSensor.
        
        - TaskInstance проверяется по комбинации dag_id + task_id + execution_date
        - execution_delta позволяет проверять задачи с другой датой
        - allowed_states по умолчанию ['success']
        """
        from airflow.models.dagrun import DagRun
        from airflow.models.taskinstance import TaskInstance
        from airflow.utils.db import create_session
        
        result = {
            'dag_id': task_config.get('external_dag_id'),
            'task_id': task_config.get('external_task_id'),
            'status': 'unknown',
            'ready': False
        }
        
        try:
            # Определяем дату выполнения
            execution_date = context.get('execution_date')
            execution_delta = task_config.get('execution_delta', timedelta())
            check_date = execution_date - execution_delta
            
            self.log.info(f"""
                Проверка задачи:
                - DAG: {result['dag_id']}
                - Task: {result['task_id']}
                - Execution date: {execution_date}
                - Check date (с учетом delta): {check_date}
            """)
            
            with create_session() as session:
                # Ищем DagRun для внешнего DAG
                dag_run = session.query(DagRun).filter(
                    DagRun.dag_id == result['dag_id'],
                    DagRun.execution_date == check_date
                ).first()
                
                if not dag_run:
                    result['status'] = 'no_dag_run'
                    self.log.info(f"  DagRun не найден для {result['dag_id']} на {check_date}")
                    return result
                
                # Ищем TaskInstance
                task_instance = session.query(TaskInstance).filter(
                    TaskInstance.dag_id == result['dag_id'],
                    TaskInstance.task_id == result['task_id'],
                    TaskInstance.run_id == dag_run.run_id
                ).first()
                
                if not task_instance:
                    result['status'] = 'no_task_instance'
                    self.log.info(f"  TaskInstance не найден для {result['task_id']}")
                    return result
                
                # Проверяем состояние задачи
                current_state = task_instance.state
                allowed_states = task_config.get('allowed_states', [State.SUCCESS])
                
                self.log.info(f"  Текущее состояние: {current_state}")
                self.log.info(f"  Ожидаемые состояния: {allowed_states}")
                
                if current_state in allowed_states:
                    result['status'] = 'success'
                    result['ready'] = True
                    self.log.info(f"  ✅ Задача готова!")
                elif current_state in [State.FAILED, State.UPSTREAM_FAILED]:
                    result['status'] = 'failed'
                    self.log.warning(f"  ❌ Задача завершилась с ошибкой!")
                elif current_state in [State.SKIPPED]:
                    result['status'] = 'skipped'
                    self.log.info(f"  ⏭ Задача пропущена")
                else:
                    result['status'] = 'running'
                    self.log.info(f"  🔄 Задача еще выполняется...")
                    
        except Exception as e:
            result['status'] = 'error'
            result['error'] = str(e)
            self.log.error(f"  Ошибка при проверке: {e}")
        
        return result
    
    def poke(self, context: Dict) -> bool:
        """
        Проверяет все внешние задачи.
        
        Возвращает True только если ВСЕ задачи готовы.
        
        - poke() вызывается автоматически через BaseSensorOperator
        - При возврате False и mode='reschedule', задача освобождает слот
        - При возврате True, сенсор считается выполненным
        """
        self.log.info(f'=' * 50)
        self.log.info(f'Проверка {len(self.external_tasks)} внешних задач')
        
        all_ready = True
        has_failed = False
        
        for i, task_config in enumerate(self.external_tasks):
            self.log.info(f'')
            self.log.info(f'Проверка {i + 1}/{len(self.external_tasks)}:')
            
            result = self._check_single_task(context, task_config)
            
            if not result['ready']:
                all_ready = False
                
            if result['status'] == 'failed':
                has_failed = True
                self.log.warning(
                    f"⚠️  Задача {result['task_id']} в DAG {result['dag_id']} "
                    f"завершилась с ошибкой!"
                )
        
        self.log.info(f'')
        if has_failed:
            # Если есть упавшие задачи - можно либо фейлить сенсор, либо ждать
            # В данной реализации продолжаем ждать (можно изменить поведение)
            self.log.warning('⚠️  Некоторые задачи упали, но сенсор продолжает ожидание')
            
        if all_ready:
            self.log.info('✅ Все внешние задачи успешно выполнены!')
            return True
        else:
            self.log.info('⏳ Ожидаем завершения всех внешних задач...')
            return False