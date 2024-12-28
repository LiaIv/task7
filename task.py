import os
import pandas as pd
from datetime import datetime

class ClientDataProcessor:
    device_translation = {
        "mobile": "с мобильного браузера",
        "tablet": "с браузера планшета",
        "laptop": "с браузера ноутбука",
        "desktop": "с браузера настольного компьютера"
    }

    def __init__(self, csv_path):
        self.csv_path = csv_path
        self.clients_df = None
        self.descriptions = []

    def load_data(self):
        try:
            self.clients_df = pd.read_csv(self.csv_path)
            print("Файл успешно загружен.")
            self.clients_df.columns = self.clients_df.columns.str.strip().str.lower()
        except Exception as e:
            raise RuntimeError(f"Не удалось загрузить файл: {e}")

    def validate_columns(self):
        required_cols = {"name", "device_type", "browser", "sex", "age", "bill", "region"}
        missing_cols = required_cols - set(self.clients_df.columns)
        if missing_cols:
            raise ValueError(f"Отсутствуют необходимые столбцы: {', '.join(missing_cols)}")

    def preprocess_data(self):
        self.clients_df = (
            self.clients_df
            .dropna(subset=["name", "sex", "age", "bill", "region"])
            .assign(
                age=lambda df: pd.to_numeric(df["age"], errors='coerce').fillna(0).astype(int),
                bill=lambda df: pd.to_numeric(df["bill"], errors='coerce')
            )
            .dropna(subset=["bill"])
        )
        print("Данные успешно обработаны.")

    def generate_descriptions(self):
        def create_description(row):
            sex = row['sex'].strip().lower()
            sex_desc = "мужского пола" if sex == 'male' else "женского пола"
            device_desc = self.device_translation.get(row['device_type'].strip().lower(), f"с устройства {row['device_type']}")
            action = "совершил" if sex == 'male' else "совершила"
            return (f"Пользователь {row['name']} {sex_desc}, {row['age']} лет "
                    f"{action} покупку на {int(row['bill'])} у.е. {device_desc} "
                    f"браузера {row['browser']}. Регион, из которого совершалась покупка: {row['region']}.")

        self.descriptions = self.clients_df.apply(create_description, axis=1).tolist()
        print("Описание пользователей успешно сгенерировано.")

    def save_descriptions(self):
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_dir = './txt-files'
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, f'client_descriptions_{timestamp}.txt')
        try:
            with open(output_file, 'w', encoding='utf-8') as file:
                file.write('\n'.join(self.descriptions))
            print(f"Описание пользователей успешно сохранено в файл: {output_file}")
        except Exception as e:
            raise RuntimeError(f"Не удалось сохранить файл: {e}")

    def process(self):
        try:
            self.load_data()
            self.validate_columns()
            self.preprocess_data()
            self.generate_descriptions()
            self.save_descriptions()
        except Exception as e:
            print(f"Ошибка выполнения программы: {e}")

if __name__ == "__main__":
    csv_file = 'web_clients_correct.csv'
    processor = ClientDataProcessor(csv_file)
    processor.process()
