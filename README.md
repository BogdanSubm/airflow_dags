# Репозиторий DAG для Airflow

Этот репозиторий содержит Directed Acyclic Graphs (DAG) для Apache Airflow.

## Инструкции по настройке

1. **Создать форк репозитория**
   Нажмите на кнопку "Fork" в правом верхнем углу, чтобы создать свою копию репозитория.

2. **Клонировать форкнутый репозиторий** 

   После создания форка, клонируйте свою версию репозитория:
   ```
   git clone <url-репозитория>  
   cd <каталог-репозитория>
   ```
   
3. **Установить зависимости**  
   Убедитесь, что у вас установлены Python и необходимые библиотеки:  

   ```
   pip install -r requirements.txt
   ```
   
4. **Поместить файлы DAG**  
   Скопируйте ваши файлы DAG в каталог `dags`.

5. **Внести изменения и коммитить**
   ```
   git add dags/*
   git commit -m "Добавлены новые файлы DAG"
   ```
   
6. **Отправить изменения в ваш форк**
   ```
   git push origin main
   ```
7. **Отправить Pull Request**
   * Перейдите на страницу вашего форка на GitHub.
   * Нажмите на кнопку "Pull Requests" и затем "New Pull Request".
   * Выберите вашу ветку и сравните с веткой main нашего репозитория.
   * Нажмите "Create Pull Request" и заполните необходимые поля.

8. **Доступ к интерфейсу Airflow**  
Перейдите по адресу `http://95.163.241.236:8080/home`, чтобы просмотреть ваши DAG.
   (есть задержка между pull request и добавлением dag в airflow примерно 5 минут)

