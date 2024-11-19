import os
import requests
import pandas as pd

from datetime import datetime, timedelta
from io import StringIO


class DebentureProcessor:
    def __init__(self, base_url, output_dir="Daily Prices", output_file="daily_prices_to_pbi.csv"):
        """
        Inicializa o processador de debêntures.
        :param base_url: URL base para download dos arquivos.
        :param output_dir: Diretório onde os arquivos baixados serão armazenados.
        :param output_file: Nome do arquivo CSV consolidado.
        """

        self.base_url = base_url
        self.output_dir = output_dir
        self.output_file = output_file
        self.header_lines = [] 
        self._ensure_directory_exists()

    def _ensure_directory_exists(self):
        """Garante que o diretório de saída exista."""

        os.makedirs(self.output_dir, exist_ok=True)

    @staticmethod
    def _get_last_weekdays(n):
        """
        Obtém as últimas N datas úteis.
        :param n: Número de dias úteis a obter.
        :return: Lista de datas úteis.
        """

        weekdays = []
        day = datetime.now()
        while len(weekdays) < n:
            day -= timedelta(days=1)
            if day.weekday() < 5:
                weekdays.append(day)
            ### remover comentario abaixo e comentar linha 40 para considerar o dia atual. 
            # day -= timedelta(days=1)  # subtrai depois de verificar
        return weekdays
        

    def _build_url(self, date):
        """
        Modifica a URL para uma data específica.
        :param date: Objeto datetime representando a data.
        :return: URL formatada.
        """

        formatted_date = date.strftime("%y%m%d")
        return f"{self.base_url}db{formatted_date}.txt"

    def _download_file(self, url, file_path):
        """
        Faz o download de um arquivo e salva no diretório especificado.
        :param url: URL do arquivo.
        :param file_path: Caminho para salvar o arquivo.
        """

        try:
            print(f"Baixando: {url}")
            response = requests.get(url)
            response.raise_for_status()
            with open(file_path, 'wb') as f:
                f.write(response.content)
            print(f"Arquivo salvo em: {file_path}")
            return True
        except requests.exceptions.RequestException as e:
            print(f"Erro ao baixar {url}: {e}")
            return False

    def download_files(self, num_days=5):
        """
        Faz o download dos arquivos para os últimos N dias úteis.
        :param num_days: Número de dias úteis a considerar.
        :return: Lista de caminhos dos arquivos baixados com sucesso.
        """

        weekdays = self._get_last_weekdays(num_days)
        downloaded_files = []

        for date in weekdays:
            formatted_date = date.strftime('%Y%m%d')
            file_path = os.path.join(self.output_dir, f"{formatted_date}.txt")
            url = self._build_url(date)
            if self._download_file(url, file_path):
                downloaded_files.append(file_path)

        return downloaded_files

    def _extract_header_and_content(self, file):
        """
        Extrai o cabeçalho e o conteúdo de um arquivo.
        :param file: Caminho do arquivo a ser processado.
        :return: Tupla contendo o cabeçalho (lista de strings) e o conteúdo (lista de strings).
        """

        with open(file, 'r', encoding='latin1') as f:
            lines = f.readlines()
        header = lines[:3]
        content = lines[3:]
        return header, content

    def _process_file(self, file):
        """
        Processa um único arquivo para obter o conteúdo como DataFrame.
        :param file: Caminho do arquivo.
        :return: DataFrame contendo o conteúdo do arquivo.
        """

        try:
            header, content = self._extract_header_and_content(file)
            if not self.header_lines:
                self.header_lines = header

            content_str = "".join(content)
            df = pd.read_csv(StringIO(content_str), delimiter="@", encoding="latin1")
            df['Data'] = os.path.basename(file).split(".")[0]
            return df
        except Exception as e:
            print(f"Erro ao processar {file}: {e}")
            return None

    def process_files(self, files):
        """
        Processa uma lista de arquivos e consolida os dados.
        :param files: Lista de arquivos a serem processados.
        :return: DataFrame consolidado com os dados.
        """

        dataframes = []
        for file in files:
            df = self._process_file(file)
            if df is not None:
                dataframes.append(df)

        if dataframes:
            return pd.concat(dataframes, ignore_index=True)
        else:
            raise ValueError("Nenhum arquivo válido foi processado.")

    def save_daily_prices_data(self, dataframe):
        """
        Salva o DataFrame com toodos os dados em um arquivo CSV, incluindo o cabeçalho.
        :param dataframe: DataFrame csv.
        """

        ### Remover esse bloco que está comentado acaba com o problema do cabeçalho e,
        # consequentemente, talvez acabe com o problema em relação ao powerBI
        
        # with open(self.output_file, 'w', encoding='latin1') as f:
        #     f.writelines(self.header_lines)
        #     f.write("\n")
        dataframe.to_csv(self.output_file, index=False, mode='a', encoding='latin1')
        print(f"Arquivo csv salvo em: {self.output_file}")

    def run(self, num_days=5):
        """
        Executa todo o processo de download, processamento e consolidação dos dados.
        :param num_days: Número de dias úteis a considerar.
        """

        try:
            files = self.download_files(num_days)
            if files:
                daily_prices_df = self.process_files(files)
                self.save_daily_prices_data(daily_prices_df)
                print("Processamento concluído com sucesso!")
            else:
                print("Nenhum arquivo foi baixado")
        except Exception as e:
            print(f"Erro durante o processamento: {e}")


if __name__ == "__main__":
    base_url = "https://www.anbima.com.br/informacoes/merc-sec-debentures/arqs/"
    processor = DebentureProcessor(base_url)
    processor.run(num_days=5)
