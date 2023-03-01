/*
 * Autor  : Choquehuallpa Hurtado Oscar Alcides
 * Fecha  : 28/02/23
 * Titulo : Generación de residuos solidos domiciliarios al año 
 *          en promedio por vivienda en areas urbanas
 * Curso  : BIG-DATA 
 * Docente: Quispe Merma Rafael Ricardo  
 * */

var residuos = (
  spark.read
    .option("header","true")
    .option("delimiter",";")
    .option("inferSchema","true")
    .csv("residuos.csv")
);


//eliminar datos innecesarios
residuos = residuos.drop("FECHA_CORTE","N_SEC","UBIGEO","POB_TOTAL","POB_RURAL")

//Modificar al formato correcto a QRESIDUOS_DOM
val formatoPunto = regexp_replace($"QRESIDUOS_DOM",",",".");
residuos = residuos.withColumn("QRESIDUOS_DOM",formatoPunto.cast("double"));

//Conviertiendo de Toneladas/año a Kilogramos/año para evitar sumar numeros reales
residuos = (
  residuos.withColumn("QRESIDUOS_DOM",($"QRESIDUOS_DOM" * 1000).cast("int"))
);


//identificar a 10 distritos que generan la mayor cantidad 
//de residuos solidos domiciliarios a largo de los 8 años
(
  residuos
    .groupBy($"DISTRITO")
    .agg((sum($"QRESIDUOS_DOM") / 1000).as("total"))
    .orderBy(desc("total"))
    .limit(10)
    .show()
)


//comparar la cantidad de residuos solidos domiciliarios 
//generados en diferentes regiones naturales en los 8 años
(
residuos
  .groupBy("REG_NAT")
  .agg((sum($"QRESIDUOS_DOM") / 1000).as("total"))
  .show()
)


//Ver la evolución de la generación de residuos solidos domiciliarios
//para el distrito de abancay a lo largo de los 8 años
(
  residuos
    .filter($"DISTRITO" === "ABANCAY")
    .groupBy("PERIODO")
    .agg((sum("QRESIDUOS_DOM") / 1000).as("total"))
    .orderBy("PERIODO")
    .show()
)


//Indentificar una posible tendencia en la generación de residuos solidos 
//domiciliarios para el distrito de abancay con el pasar de los años
var groupPerPeriod = (
  residuos
    .filter($"DISTRITO" === "ABANCAY")
    .groupBy("PERIODO")
    .agg((sum("QRESIDUOS_DOM") / 1000).as("total"))
    .orderBy("PERIODO")
)

val corr = groupPerPeriod.stat.corr("PERIODO","total");
println("la correlacion es: " + corr);


/*
*/


