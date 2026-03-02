from datetime import datetime

def python_test_func(animal: str, **context) -> None:
    print(animal)
    print(context['ds'])
    print(datetime.now())