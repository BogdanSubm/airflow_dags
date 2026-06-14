import pendulum
from typing import Any

from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.exceptions import AirflowException
from airflow.operators.branch import SkipMixin


class CustomBranchOperator(BaseOperator, SkipMixin):
    """
    смотрит на день месяца и решает идти дальше или пропустить расчёты
    active_days передаём снаружи из дага не зашиваем здесь
    """

    def __init__(
        self,
        active_days: list[int],
        active_task_id: str,
        skip_task_id: str,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.active_days = active_days
        self.active_task_id = active_task_id
        self.skip_task_id = skip_task_id

    def execute(self, context: Context) -> Any:
        dt = pendulum.parse(context['ds'])

        self.log.info(
            f"сегодня: {dt.date()}, день месяца: {dt.day}, "
            f"активные дни: {self.active_days}"
        )

        if dt.day in self.active_days:
            tasks_to_execute = [self.active_task_id]
            self.log.info(f"активный день, идём в: {self.active_task_id}")
        else:
            tasks_to_execute = [self.skip_task_id]
            self.log.info(f"неактивный день, пропускаем расчёты, идём в: {self.skip_task_id}")

        # проверяем что таски которые хотим запустить вообще существуют в даге
        valid_task_ids = set(context["dag"].task_ids)
        invalid_task_ids = set(tasks_to_execute) - valid_task_ids

        if invalid_task_ids:
            raise AirflowException(
                f"Branch callable must return valid task_ids. "
                f"Invalid tasks found: {invalid_task_ids}"
            )

        self.ski