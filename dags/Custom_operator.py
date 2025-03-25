# Шаблон кастомного оператора

from airflow.models.operator import BaseOperator
class TestOperator(BaseOperator):
    def __init__(self,type_of_table,*args,**kwargs):
        template_fields = ('type_of_table',)
        super().__init__(*args,**kwargs)
        self.type_of_table = type_of_table

    def custom_func(self):
        pass

    def execute(self,context):
        # бизнес-логика
        self.custom_func()

        print(context['ds']) # только внутри execute можно использовать context
        pass
test = TestOperator(
    task_id = 'test',
    type_of_table = '{{ ds }}'
)