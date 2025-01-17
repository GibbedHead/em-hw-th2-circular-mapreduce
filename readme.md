# Многопоточная реализация MapReduce

## Обзор

Этот проект представляет собой упрощенную реализацию распределенной системы обработки данных, вдохновленную парадигмой
MapReduce. Фреймворк предназначен для управления выполнением заданий картирования и сокращения в многопоточной среде. Он
позволяет динамически назначать и обрабатывать данные из файлов, а также эффективно обрабатывать промежуточные
результаты.

## Структура Проекта

В проекте включены следующие ключевые компоненты:

- **Worker**: Представляет рабочие узлы, выполняющие задачи map и reduce.
- **Manager**: Управляет жизненным циклом задач, включая создание, выполнение и отслеживание завершения.
- **Tasks**: Содержит три основных класса задач:
    - **Task**: Абстрактный класс, представляющий общую задачу.
    - **MapTask**: Представляет операцию map с использованием входных файлов.
    - **ReduceTask**: Обрабатывает процесс reduce для агрегирования результатов.

- **Результаты**: Классы, которые инкапсулируют результаты задач картирования и сокращения:
    - **MapTaskResult**: Содержит выходные файлы, созданные задачами map.
    - **ReduceTaskResult**: Содержит пары ключ-значение, полученные в результате выполнения задач reduce.

- **Утилитарные Классы**:
    - **FileUtil**: Предоставляет вспомогательные методы для операций с файлами и директориями.

## Установка

1. **Клонируйте репозиторий**:
   ```bash
   git clone https://github.com/GibbedHead/em-hw-th2-mapreduce.git;
   ```
2. **Запустите тесты**
   ```bash
   cd em-hw-th2-mapreduce;
   mvn test;
   ```

## Использование

1. Подготовьте Входные Файлы: Выберите свои или в архиве [test_data.zip](test_data.zip) есть набор файлов и файл с их
   списком для удобного считывания.
2. Запустите Приложение: в проекте есть [Main.java](src%2Fmain%2Fjava%2Fru%2Fchaplyginma%2FMain.java) с готовым запуском
   обработки данных из архива пункта 1. Можно "поиграть" с количеством исполнителей и reduce задач.
    ```java
    final int numWorkers = 4;
    final int numReduceTasks = 10;
    ```
3. Проверьте Выходные Данные: Финальный результат будет в файле [result.txt](result%2Fresult.txt)