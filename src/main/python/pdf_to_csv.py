import tabula
import pandas as pd
from datetime import datetime

start_time = datetime.now()

print('Starting: {}'.format(start_time))

pdf_in = "C:\\w\\EthicsForAnimals\\src\\main\\resources\\extraction_all.pdf" #Path to PDF

csv_out = "C:\\w\\EthicsForAnimals\\src\\main\\resources\\EFA_all.csv"

# to CSV
tabula.convert_into(pdf_in, csv_out, output_format="csv", pages='all')

end_time = datetime.now()

print('Done: {}'.format(end_time))
print('Duration: {}'.format(end_time - start_time))