import pandas as pd
from extract_and_save_data import connect_mongodb, connect_database, connect_collection

def visualize_collection(collection):
    for documento in collection.find():
        print(documento)
        
def rename_column(collection, column_name, new_name):
    collection.update_many({}, {'$rename': {column_name: new_name}})
    print('The column has been renamed successfully')

def select_category(collection, column_name, category):
    query = {column_name: category}
    
    query_list = []
    
    for document in collection.find(query):
        query_list.append(document)
        
    return query_list

def make_regex(collection, column, regex):
    query = {column: {'$regex': regex}}
    
    query_list = []

    for document in collection.find(query):
        query_list.append(document)
        
    return query_list

def create_dataframe(list):
    dataframe = pd.DataFrame(list)
    return dataframe

def format_date(df, column_name, date_format, new_format):
    try:
        df[column_name] = pd.to_datetime(df[column_name], format=date_format)
        df[column_name] = df[column_name].dt.strftime(new_format)
    except Exception as e:
        print(e)

def save_csv(df, path):
    df.to_csv(path)
    print('The file has been saved successfully')
    
if __name__ == '__main__':
    with open('data_base_key.txt', 'r') as file:
        uri = file.read()
    
    client = connect_mongodb(uri)
    database = connect_database(client, 'test')
    collection = connect_collection(database, 'collumn_test')
    
    rename_column(collection, 'lat', 'Latitude')
    rename_column(collection, 'lon', 'Longitude')
    
    books = select_category(collection, 'Categoria do Produto', 'livros')
    books = create_dataframe(books)
    save_csv(books, 'data-processed/book_table_script.csv')
    
    filtered_data = make_regex(collection, 'Data da Compra', '/202[1-9]')
    filtered_data = create_dataframe(filtered_data)
    save_csv(filtered_data, 'data-processed/products_sales_2021_present_script.csv')