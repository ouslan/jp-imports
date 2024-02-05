from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from yaspin import yaspin
import os
import time

class Downloader:
    def download_data(self, imports=False):
        # Set download directory to the working directory
        options = Options()
        options.add_argument("--headless")
        options.set_preference("browser.download.folderList", 2)
        options.set_preference("browser.download.dir", f'{os.getcwd()}/data/raw/')
        options.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/csv")
        
        file_num = len(os.listdir('data/raw/')) + 1

        # URL to navigate to
        url = "http://www.estadisticas.gobierno.pr/iepr/Publicaciones/Proyectosespeciales/ComercioExterno/Detalle.aspx"

        # Create a Firefox driver with the specified options
        driver = webdriver.Firefox(options=options)

        # Navigate to the URL
        driver.get(url)

        if imports:
            driver.find_element(By.XPATH, '//*[@id="dnn_ctr244936_import_export_htmltables_rbl_IE_1"]').click()
        
        # Click on the checkboxes using XPath
        for num in range(2, 23):
            driver.find_element(By.XPATH, f'//*[@id="dnn_ctr244936_import_export_htmltables_tv_codesn{num}CheckBox"]').click()

        # Click on the download button using XPath and save it to data/raw/test.csv
        driver.find_element(By.XPATH, '//*[@id="dnn_ctr244936_import_export_htmltables_btn_BajarDatos"]').click()

        while len(os.listdir('data/raw/')) > file_num or len(os.listdir('data/raw/')) == file_num - 1:
            time.sleep(1)

        for file in os.listdir('data/raw/'):
            if file.startswith('e_HTS'):
                os.rename(f'{os.getcwd()}/data/raw/' + file, f'{os.getcwd()}/data/raw/exports.csv')
            if file.startswith('i_HTS'):
                os.rename(f'{os.getcwd()}/data/raw/' + file, f'{os.getcwd()}/data/raw/import.csv')

        # Close the browse
        driver.quit()