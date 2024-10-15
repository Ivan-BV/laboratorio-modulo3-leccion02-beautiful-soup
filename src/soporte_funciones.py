from tqdm import tqdm
import pandas as pd
from bs4 import BeautifulSoup
import requests
import pandas as pd

def obtener_productos(url: str):
    """
    Extrae y organiza información de productos desde una página web paginada.

    La función envía solicitudes HTTP a una URL base específica, recorriendo 100 páginas de productos. 
    Utiliza web scraping para obtener los datos relevantes de cada producto, tales como el nombre, categoría, 
    secciones, descripción, dimensiones y enlaces a las imágenes de los productos. Finalmente, organiza 
    esta información en un DataFrame de pandas y lo devuelve.

    Parámetros:
    -----------
    url : str
        La URL base del sitio web de donde se obtendrán los productos. Se añade un número de página a la URL 
        en cada iteración para acceder a las diferentes páginas de resultados.

    Retorna:
    --------
    pd.DataFrame
        DataFrame con las columnas: 
        - 'nombre': Nombre del producto.
        - 'categoria': Categoría del producto.
        - 'sección': Secciones a las que pertenece el producto.
        - 'descripción': Descripción del producto.
        - 'dimensiones': Dimensiones del producto.
        - 'imagenes': URLs de las imágenes de los productos.

    Excepciones:
    ------------
    len(lista_productos) == 0 :
        Si la lista de productos está vacía, indica que no hay más productos disponibles.

    ConnectionError:
        Si ocurre un error de conexión, se imprime el código de estado de la respuesta HTTP.

    Notas:
    ------
    - La función está diseñada para manejar un máximo de 100 páginas de productos.
    - Se utiliza BeautifulSoup para analizar el contenido HTML y pandas para organizar los datos en un DataFrame.
    - La función elimina columnas irrelevantes y reestructura el DataFrame para que solo incluya las columnas
      necesarias.
    - Los datos que no están disponibles, como las imágenes inexistentes, se rellenan con valores nulos (pd.NA).
    - Si hay un problema de conexión o si la lista de productos está vacía, la función maneja estos casos 
      mediante excepciones.
    """
    try:
        # Genero el data frame el cual va almacenar todos los productos
        df_final = pd.DataFrame()

        # Bucle de 1 hasta 101 para iterar sobre las 100 primeras páginas
        for pagina in tqdm(range(1,101)):

            # Inicializamos la conexión
            respuesta = requests.get(f"{url}{pagina}", timeout=10)
            sopa_atrezzo = BeautifulSoup(respuesta.content, "html.parser")

            # Busco, obtengo y parseo el html de la etiqueta div la cual es padre de todos los elementos que necesitamos (Otra manera de hacerlo es ir encontrando etiqueta a etiqueta)
            lista_productos = sopa_atrezzo.findAll("div", {"class": "product-slide-entry shift-image"})
            sopa_atrezzo = BeautifulSoup(respuesta.content, "html.parser")

            # Obtenemos la lista de los productos
            lista_productos = sopa_atrezzo.findAll("div", {"class": "product-slide-entry shift-image"})

            # Obtengo la url de las imagenes en caso no existir imagén de producto retorno un nulo. En este caso concateno el inicio de la url hasta la barra después del .es con el string
            # que me devuelve get src para obtener el enlace completo a la imagen
            imagenes = ["https://atrezzovazquez.es/"+producto.find("img").get("src") if producto.find("img") is not None else pd.NA for producto in lista_productos]

            # Obtengo los productos
            productos = [producto.getText() for producto in lista_productos]

            # Obtengo las secciones para el posterior tratamiento de estas
            secciones = [seccion.findAll("div", {"class": "cat-sec"}) for seccion in lista_productos]

            # Itero sobre las secciones que son listas de subsecciones las cuales contienen el Texto a extraer
            secciones_formato_lista = [[subseccion.getText().strip() for subseccion in seccion] for seccion in secciones]

            # Meto los productos obtenidos que entran en sucio con saltos de linea y todo junto
            df = pd.DataFrame(productos, columns=["columna_inicial"])

            # Hago el split por los saltos de linea y utilizo expand para crear las nuevas columnas
            df = df["columna_inicial"].str.split("\n", expand=True)

            # Elimino las columnas que no sirven. Un paso a tener en cuenta en un futuro es hacerlo "Automatico" detectando las columnas vacías
            df.drop(columns=[0, 1, 2, 3, 4, 7, 9, 10, 12, 13, 15, 16], inplace=True, axis=1)

            #Añado las secciones
            secciones_nuevas = []

            # Bucle para iterar sobre la lista de secciones. Y dentro otro bucle para iterar por cada elemento en la posición de las secciones
            for secciones in secciones_formato_lista:
                string = ""

                # Utilizo un contador para saber cuando separar y cuando no. La idea principal era utilizarlo para poner saltos de linea
                contador = 0
                for seccion in secciones:
                    string += seccion
                    contador += 1
                    if contador != len(secciones):
                        string+=" "
                secciones_nuevas.append(string)
            df["sección"] = secciones_nuevas

            # Con regex hago split del nombre del producto, lo agrupo para extraerlo y con expand creo una nueva columna
            df_nuevo = df[5].str.split(r"([A-Z]*\d{1,3})", expand=True)

            # Concateno por columnas con axis 1
            df = pd.concat([df, df_nuevo], axis=1)

            # Elimino las columnas que ya no me sirven y con inplace hago que el cambio sea permanente
            df.drop(columns=[5, 6, 14, 2], inplace=True)

            # Renombro y ordeno las columnas
            df.rename(columns={0: "categoria", 1: "nombre", 8: "descripción", 11: "dimensiones"}, inplace=True)
            df = df.reindex(columns=["nombre", "categoria", "sección", "descripción", "dimensiones"])

            # Por último añado las imagenes
            df["imagenes"] = imagenes
            df_final = pd.concat([df_final, df], axis=0)
            #Aquí podemos poner un sleep(1) como buena práctica pero para este caso considero que no hace falta

        # Utilizo reset index para eliminar los index generados con las concatenaciones anteriores
        df_final = df_final.reset_index()

        # Esto me genera un columna index la cual elimino porque no nos sirve
        df_final.drop(columns=["index"], inplace=True)
        return df_final
    except len(lista_productos)==0:
        print("La lista esta vacía por lo que se han terminado los productos")
    except ConnectionError:
        print(f"Error de conexión:\nStatus code: {respuesta.status_code}")