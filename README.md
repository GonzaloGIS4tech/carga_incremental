Instrucciones para instalar la librería

-------------------------------------------------



En la *terminal (teniendo activado el entorno de anaconda/vscode)* es necesario realizar cd .. hasta llegar a C. 



Escribe Z: para entrar en el disco Z y, a continuación, introduce el comando *cd Z:\grupo_almacenamiento\desarrollos_internos\_PROCESOS_INTERNOS\herramientas\load_data_to_posgres\ld_lib*



Una vez en el directorio, escribe el siguiente comando (la instalación es compatible con Python 3.x.):
```cmd
	pip install dist/loaddata-0.1.0-py3-none-any.whl
```


Desde este momento, puedes acceder a los módulos de la función loaddata mediante importación del entorno en el que has realizado la operación. 
Para importarlo en tus scripts:
```python
	from loaddata import LoadData
	ld = LoadData()
	# aquí está el resto del código


	ld.load_all_data() # aquí introduces los parámetros para realizar la carga incremtal/truncado
```
#### DEBES TENER EN CUENTA QUE, PARA QUE LA FUNCIÓN NO FALLE, Y EN CASO DE QUE YA EXISTA LA TABLA EN BASE DE DATOS, DEBE TENER EL CAMPO unique_id. PARA CREARLO HAZ LO SIGUIENTE:
```sql
	ALTER TABLE
	tu_tabla
	ADD COLUMN unique_id BIGINT; -- SI HAS ESPECIFICADO UN ID ÚNICO, ASEGÚRATE DE QUE ESTE PARÁMETRO SEA DEL MISMO TIPO PARA EVITAR PROBLEMAS CON MERGE
	UPDATE tu_tabla
	SET unique_id = -1;

	DESPUÉS DE EJECUTAR LA FUNCIÓN:

	DELETE FROM tu_tabla WHERE unique_id = -1
```

