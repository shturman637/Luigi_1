
import luigi
import os
import wget
import tarfile
import pandas as pd
import io
import logging
import gzip
import glob
import shutil
from io import StringIO
from pathlib import Path

# Класс задачи для скачивания GEO-данных
class DownloadDataset(luigi.Task):
    # Параметры задачи
    dataset_id = luigi.Parameter(default='GSE68849')
    download_dir = luigi.Parameter(default='data')  # Каталог для хранения данных

    # Метод для определения выходного файла
    def output(self):
        # Определяем путь к загружаемому файлу
        return luigi.LocalTarget(os.path.join(self.download_dir, f"{self.dataset_id}_RAW.tar"))

    # Метод для выполнения задачи
    def run(self):
        # Создаем каталог, если он не существует
        os.makedirs(self.download_dir, exist_ok=True)

        # Формируем URL для загрузки
        url = f"ftp://ftp.ncbi.nlm.nih.gov/geo/series/GSE68nnn/{self.dataset_id}/suppl/{self.dataset_id}_RAW.tar"

        # Загружаем файл
        print(f"Начинаем загрузку: {url}")
        wget.download(url, out=self.output().path)
        print(f"Загрузка завершена и сохранена в {self.output().path}")


# Определяем задачу, которая извлекает файлы из архива и выполняет их обработку
class ExtractAndProcessFiles(luigi.Task):
    # Задаем параметры задачи с базовыми значениями
    dataset_id = luigi.Parameter(default='GSE68849')  # Идентификатор набора данных
    download_dir = luigi.Parameter(default='data')    # Директория для загрузки архивов
    output_dir = luigi.Parameter(default='processed_data')  # Директория для сохранения обработанных данных

    # Указываем зависимости задачи. Перед извлечением и обработкой необходимо скачать данные
    def requires(self):
        # Задача DownloadDataset должна быть выполнена до того, как начнется выполнение текущей задачи
        # Передаем идентификатор набора данных и папку для загрузки
        return DownloadDataset(self.dataset_id, self.download_dir)

    # Указываем, куда будет сохраняться результат выполнения задачи
    def output(self):
        # Luigi будет проверять директорию output_dir на наличие результата выполнения
        return luigi.LocalTarget(self.output_dir)

    # Основной метод, выполняющий задачу
    def run(self):
        # Создаем директорию для сохранения обработанных данных, если она не существует
        os.makedirs(self.output_dir, exist_ok=True)

        # Получаем путь к tar-архиву скачанного набора данных
        tar_path = self.input().path
        logging.info(f"Opening TAR file at {tar_path}")

        # Проверяем, существует ли tar-файл
        if not os.path.isfile(tar_path):
            logging.error(f"TAR file does not exist: {tar_path}")
            raise FileNotFoundError(f"TAR file does not exist: {tar_path}")

        # Открываем tar-файл для чтения
        with tarfile.open(tar_path, 'r') as tar:
            logging.info(f"Extracting files from {tar_path}")

            # Перебираем все файлы внутри архива
            for member in tar.getmembers():
                # Проверяем, является ли текущий объект файлом и заканчивается на '.txt.gz'
                if member.isfile() and member.name.endswith('.txt.gz'):
                    # Формируем название файла без расширения
                    member_name = os.path.splitext(member.name)[0]

                    # Создаем директорию для извлеченного файла
                    extraction_dir = os.path.join(self.output_dir, member_name)
                    os.makedirs(extraction_dir, exist_ok=True)

                    logging.info(f"Extracting file {member.name} to {extraction_dir}")

                    # Извлекаем содержимое файла из архива
                    with tar.extractfile(member) as f:
                        # Открываем файл как Gzip (сжатый файл .gz)
                        with gzip.GzipFile(fileobj=f) as gz:
                            # Читаем содержимое файла
                            file_content = gz.read()

                            # Формируем полный путь для сохранения распакованного файла
                            output_file_path = os.path.join(extraction_dir, member_name)
                            with open(output_file_path, 'wb') as out_file:
                                # Записываем содержимое файла на диск
                                out_file.write(file_content)

                    logging.info(f"File extracted and saved: {output_file_path}")

                    # Передаем извлеченный файл на обработку
                    self.process_file(output_file_path)

        logging.info("All files extracted and processed.")

    # Метод для обработки извлеченного файла
    def process_file(self, file_path):
        # Создаем словарь для хранения обработанных данных (разных таблиц/сегментов)
        dfs = {}

        # Открываем файл для чтения
        with open(file_path) as f:
            write_key = None  # Ключ, указывающий текущую секцию [NAME]
            fio = StringIO()  # Буфер для временного хранения содержимого секции

            # Читаем файл построчно
            for line in f.readlines():
                # Если строка начинается с '[', значит, это начало новой секции
                if line.startswith('['):
                    # Если предыдущая секция уже была записана, сохраняем её данные
                    if write_key:
                        fio.seek(0)  # Перемещаем указатель в начало буфера
                        # Если секция - заголовок 'Heading', не добавляем заголовки к таблице
                        header = None if write_key == 'Heading' else 'infer'
                        # Читаем данные в Pandas DataFrame
                        dfs[write_key] = pd.read_csv(fio, sep='\t', header=header)

                    # Очищаем буфер и обновляем текущий ключ (название новой секции)
                    fio = StringIO()
                    write_key = line.strip('[]\n')
                    continue

                # Если текущий ключ существует, записываем строки в буфер
                if write_key:
                    fio.write(line)

            # Если последняя секция завершена, сохраняем ее данные
            fio.seek(0)
            if write_key:
                dfs[write_key] = pd.read_csv(fio, sep='\t')

        # Сохраняем все обработанные секции в отдельные файлы
        for key, df in dfs.items():
            # Формируем имя выходного файла для текущей секции
            output_file_name = f"{os.path.splitext(os.path.basename(file_path))[0]}_{key}.tsv"
            output_file_path = os.path.join(os.path.dirname(file_path), output_file_name)

            # Сохраняем DataFrame в формате TSV
            df.to_csv(output_file_path, sep='\t', index=False)
            logging.info(f"Saved processed file: {output_file_path}")


class TrimProbesTable(luigi.Task):
    dataset_id = luigi.Parameter(default='GSE68849')
    download_dir = luigi.Parameter(default='data')  # Каталог для хранения данных
    processed_data_dir = luigi.Parameter(default='processed_data')  # Директория с обработанными данными
    output_dir = luigi.Parameter(default='probes_data')  # Каталог для сохранения урезанных данных

    def requires(self):
        return ExtractAndProcessFiles(self.dataset_id)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.output_dir, 'Probes_files.tsv'))

    def run(self):
        os.makedirs(self.output_dir, exist_ok=True)

        # Список для хранения путей к найденным файлам *_Probes.tsv
        probes_files = []

        # Обход каталога processed_data_dir для поиска файлов *_Probes.tsv
        for root, dirs, files in os.walk(self.processed_data_dir):
            for file in files:
                if file.endswith('_Probes.tsv'):
                    probes_files.append(os.path.join(root, file))

        if not probes_files:
            logging.error(f"No Probes files found in the directory: {self.processed_data_dir}")
            raise FileNotFoundError(f"No Probes files found in the directory: {self.processed_data_dir}")

        # Список колонок для удаления
        columns_to_remove = [
            'Definition',
            'Ontology_Component',
            'Ontology_Process',
            'Ontology_Function',
            'Synonyms',
            'Obsolete_Probe_Id',
            'Probe_Sequence'
        ]

        # Предполагается, что мы обрабатываем каждый найденный файл
        for probes_file_path in probes_files:
            logging.info(f"Loading Probes file: {probes_file_path}")

            # Загружаем таблицу Probes
            probes_df = pd.read_csv(probes_file_path, sep='\t')

            # Удаляем ненужные колонки
            trimmed_probes_df = probes_df.drop(columns=columns_to_remove, errors='ignore')

            # Формирование имени выходного файла
            trimmed_file_path = os.path.join(os.path.dirname(self.output().path),
                                             os.path.basename(probes_file_path).replace('_Probes.tsv',
                                                                                        '_trimmed_Probes.tsv'))

            # Сохраняем урезанную таблицу
            # output_file_path = self.output().path
            trimmed_probes_df.to_csv(trimmed_file_path, sep='\t', index=False)
            logging.info(f"Trimmed probes table saved to: {trimmed_file_path}")

        # Сигнализируем о завершении работы
        with self.output().open('w') as f:
            f.write("Trimmed Probes processing completed.\n")


class DeleteOriginalData(luigi.Task):
    dataset_id = luigi.Parameter(default='GSE68849')  # Идентификатор набора данных
    download_dir = luigi.Parameter(default='data')  # Каталог для хранения загруженных данных
    processed_data_dir = luigi.Parameter(default='processed_data')  # Директория с обработанными данными
    output_dir = luigi.Parameter(default='trimmed_probes')  # Каталог для сохранения урезанных данных

    def requires(self):
        # Задача зависит от `TrimProbesTable`, который должен быть выполнен до удаления данных
        return TrimProbesTable(self.dataset_id)

    def output(self):
        # Указываем выходной файл/метку завершения задачи
        return luigi.LocalTarget(os.path.join(self.processed_data_dir, 'delete_original_data.done'))

    def run(self):
        original_items = []  # Список для хранения всех файлов и директорий, которые будут удалены

        # Проходим по дереву директорий и собираем все файлы и папки
        for root, dirs, files in os.walk(self.processed_data_dir):
            for dir in dirs:  # Добавляем директории в список
                original_items.append(os.path.join(root, dir))
            for file in files:  # Добавляем файлы в список
                original_items.append(os.path.join(root, file))

        # Инициализируем логгер для вывода сообщений
        logger = logging.getLogger('luigi-interface')
        logger.info(f"Found original files: {original_items}")  # Логируем найденные элементы

        if not original_items:
            # Если файлы не найдены, выводим предупреждающее сообщение и завершаем задачу
            logging.warning("No original files found to delete.")
            return

            # Удаление найденных файлов
        for file in original_items:
            if os.path.isfile(file):  # Проверяем, что это файл
                logging.info(f"Preparing to delete file: {file}")  # Логируем подготовку к удалению файла
                try:
                    os.remove(file)  # Удаляем файл
                    logging.info(f"File deleted successfully: {file}")  # Логируем успешное удаление
                except Exception as e:
                    # Логируем ошибку, если файл не удалось удалить
                    logging.error(f"Error deleting file {file}: {e}")

        # Удаление пустых директорий после удаления файлов
        for root, dirs, files in os.walk(self.processed_data_dir, topdown=False):
            for dir in dirs:  # Проходим по директориям
                dir_path = os.path.join(root, dir)
                try:
                    os.rmdir(dir_path)  # Пытаемся удалить пустую директорию
                    logging.info(f"Directory deleted successfully: {dir_path}")  # Логируем успешное удаление
                except OSError as e:
                    # Логируем предупреждение, если директорию не удалось удалить (например, если она не пуста)
                    logging.warning(f"Directory not empty or could not be deleted: {dir_path}, Error: {e}")

        # Проверяем, является ли `processed_data_dir` пустым, и пытаемся удалить его
        try:
            os.rmdir(self.processed_data_dir)  # Удаляем корневую директорию, если она пустая
            logger.info(
                f"Удален пустой основной каталог: {self.processed_data_dir}")  # Логируем успешное удаление каталога
        except OSError as e:
            # Логируем предупреждение, если удалить корневую директорию не удалось
            logger.warning(f"Не удалось удалить основной каталог {self.processed_data_dir}: {e}")


if __name__ == '__main__':
    luigi.run()