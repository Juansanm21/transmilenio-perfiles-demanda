# Resultados clave del proyecto (ficha para el documento)

Todas las cifras de esta ficha salen de los notebooks, del benchmark de Dask, de la tabla de AWS o de los registros del flujo; los textos de hallazgos y conclusiones están copiados de los notebooks. Es la fuente para escribir el documento sin volver a buscar números.

**Pregunta de investigación:** ¿Qué estaciones de la troncal de TransMilenio requieren refuerzo de personal de apoyo para ordenar el ingreso de usuarios, según su demanda por franja horaria?

## 1. Datos (puntos a y b de la rúbrica)

| Dato | Valor | Dónde |
|---|---|---|
| Fuente | Validaciones del componente troncal de TransMilenio (datos abiertos) | notebook 01 |
| Archivos | 21 ZIP diarios: 3 a 16 de agosto y 14 a 20 de septiembre de 2026 | notebook 01 |
| Tamaño | 1,98 GB comprimidos; 9,62 GB de CSV sin comprimir; 1,27 GB en Parquet | notebook 01, `docs/benchmark_dask.csv` |
| Filas | 31.530.646 validaciones | notebook 01 |
| Unidad de análisis | Estación x día x hora (en AWS: semana x estación x hora) | notebooks 03 y 04 |
| Variable objetivo | `validaciones` por estación, día y hora (flujo de entrada de personas) | notebook 03 |
| Días analizados | 14 días hábiles; el sábado (62 % de un día hábil), el domingo y el festivo (30 %) son otro régimen | notebook 03 y 04 |

### Decisiones de limpieza (`docs/decisiones_limpieza.csv`)

| problema | dónde | a cuánto afecta | decisión |
|---|---|---|---|
| Columnas 100 % vacías | Ruta, ID_Vehiculo, Tipo_Vehiculo | 100 % de las filas | No se usan al analizar (se conservan en el consolidado) |
| Columnas con un solo valor | Fase, Hora_Pico_SN, Operador, Sistema | todas las filas | No se usan; la hora pico se define desde la hora de la validación |
| Day_Group_Type solo distingue domingos | Day_Group_Type | 5.22 % de las filas son 'Dia 2' | El tipo de día (hábil, sábado, domingo, festivo) se construye desde la fecha |
| Valores faltantes en Linea | Linea | 2,438 filas (0.008 %) | Se dejan como 'sin línea' |
| Fecha guardada como texto | Fecha_Transaccion | todas las filas | Convertir a fecha-hora; extraer fecha, hora y día de la semana |
| Colas de medianoche (días incompletos) | 2026-08-17, 2026-09-13, 2026-09-21 | 325 filas | Excluir de los promedios por tipo de día |
| Validaciones en el archivo de otro día | Fecha_Clearing vs Fecha_Transaccion | 2,830 filas (0.009 %) | Usar la fecha real de la validación, no la del archivo |
| Código y nombre de estación pegados | Estacion_Parada | todas las filas | Separar en código de 5 dígitos y nombre |
| Un código con dos nombres | 09110 | 187,604 filas | Contar por código y usar el nombre más frecuente |
| Estaciones sin coordenadas | 17 estaciones (temporales, corrales, ampliaciones) | 2,600,589 filas (8.25 %) | Se usan en la demanda; fuera del análisis geográfico (o ubicarlas a mano) |
| Dos archivos de estaciones | estaciones_troncales.csv y _ui.csv | el archivo 2 no agrega estaciones | Usar el archivo 1 (trae la troncal) |
| Perfil y tipo de tarjeta repiten información | Nombre_Perfil y Tipo_Tarjeta | 'Anonymous' = 'tullave Básica' (29.85 % de las filas) | Usar una sola de las dos para identificar la tarjeta anónima |
| Valor = 0 | Valor | 4,377,978 filas (13.885 %) | NO se descartan: se exploran (¿transbordos? ¿pasajes gratuitos?) |
| Saldo previo = 0 | Saldo_Previo_a_Transaccion | 1,660,336 filas (5.266 %) | NO se descartan: se exploran |

## 2. Hallazgos del EDA (notebook 03)

### Hallazgos del EDA

| # | Hallazgo | Sección |
|---|---|---|
| 1 | El día hábil es un régimen propio: sábado ~62 %, domingo y festivo ~30 % de un hábil, y con otra forma de curva (dos picos solo en hábiles). | 2-3 |
| 2 | Un día hábil se parece mucho a otro: la variación entre días en la misma hora es de 2-5 % del promedio. | 3 |
| 3 | La demanda es un **conteo muy asimétrico** (mediana 317, promedio 653, máximo 15.308 por estación-hora) y con más varianza que un Poisson. | 4 |
| 4 | **27 de 154 estaciones reúnen la mitad** de las validaciones (Gini 0,48); las más grandes son portales y cabeceras. | 4 |
| 5 | Las estaciones se dividen en "de mañana" (57; periferia y portales), "de tarde" (72; centro y norte) y "doble pico" (20). | 4, 6 |
| 6 | Los portales reciben 60-85 % por **alimentación** (oleadas de buses zonales); el resto de las estaciones, casi nada. | 4 |
| 7 | La mezcla de usuarios casi no cambia entre punta y valle; el cobro $0 (tarifa 2) está en estaciones puntuales y baja en la tarde. | 5 |
| 8 | **La punta de la mañana concentra la carga en pocos puntos** (portales, 7-10 mil por hora); la **de la tarde está más repartida** (máximo 5,4 mil en Calle 100). | 6 |
| 9 | **No todos vuelven en TransMilenio:** 52 % de quienes entran en la punta de la mañana vuelve a validar en la tarde; el flujo es periferia -> centro (mañana) y centro -> periferia (tarde); el destino dominante es Calle 100. | 7 |
| 10 | El 10 % de las tarjetas (habituales, 12-14 días) hace el 32 % de las validaciones. | 7 |
| 11 | **La combinación estación x hora explica ~98 %** de la variación (en logaritmo); lo que sobra es un efecto del día de la semana (lunes bajo, viernes alto) y un nivel común del día. | 8 |
| 12 | Cinco estaciones abren o cierran entre agosto y septiembre: cambian el nivel y complican la validación temporal. | 8 |

### Hipótesis

| # | Hipótesis | Evidencia | Qué se hace con ella |
|---|---|---|---|
| H1 | La demanda de una estación-hora en un día hábil es **muy predecible con su propio historial**. | Hallazgos 2 y 11. | Línea base: promedio histórico de la estación-hora. Es lo que el modelo debe superar. |
| H2 | **El día de la semana** aporta una mejora pequeña pero real (lunes -5 a -8 %, viernes +4 a +6 %). | Sección 8 (iteración). | Variable `dia_semana`; se prueba si mejora frente a la línea base. |
| H3 | Los **portales** tienen presión en oleadas por alimentación, no continua. | Hallazgo 6. | Características `pct_alimentacion` y accesos activos; análisis aparte de los portales. |
| H4 | La **demanda de la tarde en el centro** depende de lo que entró por la mañana en los portales de origen. | Hallazgo 9. | Se contrasta en el modelo (variable: entradas de la mañana en los orígenes del flujo). |
| H5 | El **perfil de usuario** no explica cuándo hay presión (mezcla estable). | Hallazgo 7. | No entra como predictor principal; sí `pct_tarifa2`/`pct_basica` como atributos de estación (sin usarlos juntos). |
| H6 | Los **cambios de red** (estaciones nuevas, cerradas o temporales) rompen la validación temporal. | Hallazgo 12. | Se marcan y se evalúa el modelo con y sin ellas; en el flujo de Prefect hay que detectarlas. |
| H7 | La **hora de alta presión** (por encima de un umbral) se concentra en pocas estaciones-hora y es más extrema en la mañana. | Tabla de umbrales y hallazgo 8. | Se define el umbral con la operación y se evalúa el modelo también como clasificación (¿hora de alta presión?). |

### Pregunta analítica definitiva

> **¿Qué estaciones de la troncal de TransMilenio requieren refuerzo de personal de apoyo para ordenar el ingreso de usuarios, según su demanda por franja horaria?**

**Objetivo general:** identificar y anticipar, con el historial de días anteriores, las estaciones con mayor demanda de ingreso en cada franja horaria, para orientar el refuerzo del personal de apoyo que ordena el ingreso de usuarios.

**Objetivos específicos:**
1. Describir cómo se distribuye la demanda de ingreso a lo largo de la semana y del día, y justificar el análisis solo con días hábiles.
2. Ordenar las estaciones y franjas por demanda y definir, con la operación, el umbral de alta presión (punto de partida: 1.000 validaciones por hora).
3. Construir un modelo que anticipe la demanda por estación y hora y compararlo con el promedio histórico mediante validación temporal.
4. Entregar a la gerencia un ranking de estaciones y franjas y un tablero. El margen de incertidumbre del pico (intervalo del 95 % del promedio de 14 días) se reporta en el análisis del notebook 04.

- **Unidad de análisis:** estación x día x hora (en días hábiles).
- **Variable objetivo:** `validaciones` (conteo); y su versión binaria "hora de alta presión" (validaciones por encima de un umbral definido con la operación).
- **Predictores permitidos:** historial de la misma estación-hora (`lag_1`, `media_hist`), hora, día de la semana, demanda del sistema en esa hora y atributos de la estación que no usen el mismo día (tipo, `% alimentación`, forma del día, distancia al centro). **Prohibido:** cualquier medida calculada con el día que se predice (`val_dia` de los mismos 14 días, por ejemplo).
- **Modelo:** regresión (conteo) con **línea base** = promedio histórico de la estación-hora; Random Forest y LightGBM (y una regresión de conteos como referencia); métricas MAE, WAPE y RMSE; **validación temporal** (entrenar con unos días y probar con días posteriores, nunca mezclar al azar). Si no supera la línea base por un margen útil, **se dice** y la línea base queda como respuesta.
- **Producto para la gerencia:** ranking de estaciones y horas con mayor probabilidad de alta presión, para decidir **dónde y cuándo** poner personal de apoyo, y un tablero con el resultado.

### Información adicional necesaria (punto c de la rúbrica)

| Información | Estado | Para qué serviría |
|---|---|---|
| Calendario de festivos | Usado (1 festivo en el periodo) | Separar días hábiles de festivos |
| Coordenadas de estaciones | Faltan 14 estaciones activas (8,7 % de las validaciones) | Mapa y distancia al centro |
| Aperturas, cierres y estaciones temporales | No hay un archivo; se infiere de los datos (5 casos) | Evitar mezclar regímenes |
| Eventos masivos, obras, paros, clima | No disponible | Explicar días atípicos |
| Capacidad y frecuencia de buses, ocupación | No disponible | Pasar de "validaciones" a "aglomeración" |
| Datos del servicio zonal | Fuera del alcance (solo troncal) | Entender las oleadas de alimentación |

### Limitaciones

- **Solo 14 días hábiles** en tres semanas separadas por un mes; no se ve estacionalidad larga ni efectos de calendario más allá de un festivo.
- La "necesidad de personal" **no se observa**: se infiere de la demanda con un umbral.
- Las validaciones miden **entradas**, no la gente que está dentro de la estación ni las filas reales.
- El destino de los viajes es inferido; no hay transbordos internos ni otros medios.

**Siguiente paso:** el notebook 04 (ranking de estaciones y personal de apoyo); el Teorema del Límite Central con la hora de validación queda como anexo; luego el flujo con Prefect y Dask que construye la tabla de la unidad de análisis, su carga a AWS (solo resultados) y el modelo.

## 3. Ranking de estaciones y ventanas de refuerzo (puntos d, f y g)

Resultados que el flujo guarda en AWS (tabla `analisis_estacion_hora`): promedio de los días hábiles de cada semana; ventana = horas de la punta con al menos 50 % del pico de la estación (y 1.000 o más); personas en el pico = pico / 1.500, hacia arriba.

| semana | estaciones con ventana de mañana | estaciones con ventana de tarde | validaciones de un día hábil típico |
|---|---|---|---|
| 2026-08-03 | 44 | 71 | 1.856.992 |
| 2026-08-10 | 42 | 67 | 1.820.300 |
| 2026-09-14 | 44 | 71 | 1.880.420 |

**Semana del 2026-08-03, punta de la mañana**

| puesto | estación | pico (val/h) | hora pico | carga sobre umbral | personas en el pico | ventana |
|---|---|---|---|---|---|---|
| 1 | Portal Américas | 14929 | 6 | 38.092 | 10 | 5:00-8:00 |
| 2 | Portal Sur JFK Coop. Financiera | 12285 | 6 | 30.607 | 9 | 5:00-8:00 |
| 3 | Portal Suba | 11315 | 6 | 29.678 | 8 | 5:00-8:00 |
| 4 | Cabecera Usme | 10573 | 6 | 26.566 | 8 | 5:00-8:00 |
| 5 | Portal Norte - Unicervantes | 9550 | 7 | 25.604 | 7 | 6:00-9:00 |
**Semana del 2026-08-03, punta de la tarde**

| puesto | estación | pico (val/h) | hora pico | carga sobre umbral | personas en el pico | ventana |
|---|---|---|---|---|---|---|
| 1 | Calle 100 - Marketmedios | 7922 | 17 | 18.600 | 6 | 16:00-19:00 |
| 2 | Portal Norte - Unicervantes | 5266 | 17 | 12.315 | 4 | 16:00-19:00 |
| 3 | Calle 76 | 5703 | 17 | 11.688 | 4 | 16:00-19:00 |
| 4 | Portal El Dorado - C.C. NUESTRO BOGOTA | 5506 | 17 | 10.398 | 4 | 16:00-19:00 |
| 5 | Temporal Flores | 5168 | 17 | 9.866 | 4 | 16:00-19:00 |
**Semana del 2026-08-10, punta de la mañana**

| puesto | estación | pico (val/h) | hora pico | carga sobre umbral | personas en el pico | ventana |
|---|---|---|---|---|---|---|
| 1 | Portal Américas | 14693 | 6 | 39.022 | 10 | 5:00-8:00 |
| 2 | Portal Sur JFK Coop. Financiera | 11922 | 6 | 30.030 | 8 | 5:00-8:00 |
| 3 | Portal Suba | 11462 | 6 | 29.239 | 8 | 5:00-8:00 |
| 4 | Cabecera Usme | 10376 | 6 | 25.839 | 7 | 5:00-8:00 |
| 5 | Portal Norte - Unicervantes | 9305 | 7 | 25.386 | 7 | 5:00-9:00 |
**Semana del 2026-08-10, punta de la tarde**

| puesto | estación | pico (val/h) | hora pico | carga sobre umbral | personas en el pico | ventana |
|---|---|---|---|---|---|---|
| 1 | Calle 100 - Marketmedios | 7353 | 17 | 16.944 | 5 | 16:00-19:00 |
| 2 | Portal Norte - Unicervantes | 5307 | 17 | 12.629 | 4 | 16:00-19:00 |
| 3 | Calle 76 | 5288 | 17 | 11.532 | 4 | 16:00-19:00 |
| 4 | Portal El Dorado - C.C. NUESTRO BOGOTA | 5357 | 17 | 9.933 | 4 | 16:00-19:00 |
| 5 | Temporal Flores | 4996 | 17 | 9.363 | 4 | 16:00-19:00 |
**Semana del 2026-09-14, punta de la mañana**

| puesto | estación | pico (val/h) | hora pico | carga sobre umbral | personas en el pico | ventana |
|---|---|---|---|---|---|---|
| 1 | Portal Américas | 13717 | 6 | 36.839 | 10 | 5:00-8:00 |
| 2 | Portal Suba | 11426 | 6 | 29.481 | 8 | 5:00-8:00 |
| 3 | Portal Sur JFK Coop. Financiera | 10908 | 6 | 27.286 | 8 | 5:00-8:00 |
| 4 | Cabecera Usme | 10423 | 6 | 26.148 | 7 | 5:00-8:00 |
| 5 | Portal Norte - Unicervantes | 9346 | 6 | 25.655 | 7 | 6:00-9:00 |
**Semana del 2026-09-14, punta de la tarde**

| puesto | estación | pico (val/h) | hora pico | carga sobre umbral | personas en el pico | ventana |
|---|---|---|---|---|---|---|
| 1 | Calle 100 - Marketmedios | 7720 | 17 | 17.901 | 6 | 16:00-19:00 |
| 2 | Portal Norte - Unicervantes | 4982 | 17 | 11.715 | 4 | 16:00-19:00 |
| 3 | Calle 72 - Areandina | 5393 | 17 | 11.175 | 4 | 16:00-19:00 |
| 4 | Portal El Dorado - C.C. NUESTRO BOGOTA | 5487 | 17 | 10.278 | 4 | 16:00-19:00 |
| 5 | Museo Nacional - FNG | 4595 | 17 | 8.426 | 4 | 16:00-19:00 |

## 4. Propuestas de personal de apoyo (notebook 04, sección 5)

Con una persona por cada 1.500 validaciones por hora (escenario). Las 93 estaciones en alta presión necesitan 939 persona-horas por día; 610 caen en las ventanas de la mañana y de la tarde.

| Propuesta | Personas fijas | Horas por semana por persona fija | Necesidad del día cubierta | Contrato que exige |
|---|---|---|---|---|
| A. Núcleo por ventana en la misma estación | 152 | 16 (máximo 35) | 65 % | Tiempo parcial |
| B. Núcleo compartido entre estaciones | 81 | 29 (máximo 35) | 65 % | Turno partido con pausa de 7 h y cambio de estación |
| **C. Turnos continuos (recomendada)** | **53** (48 turnos fijos de 8 h y 5 de relevo) | 40 | 100 % | Tiempo completo y medio tiempo por llamado (219 turnos variables de 4 h por día) |

- Propuesta C: 1.260 horas pagadas para 939 horas necesarias (utilización de 75 %). La mezcla se mantiene con un recargo de la hora variable de 0 %, 33 % o 60 %; con 100 % todo pasa a turnos fijos (267 turnos, utilización de 44 %).
- Propuesta B: 81 parejas con las dos estaciones a una distancia mediana de 5,6 km (máximo 13,4 km entre las que tienen coordenadas).
- Capacidad por persona de 1.000, 1.500 o 2.000: la propuesta A pasa a 226, 152 o 119 personas fijas y el núcleo siempre cubre entre 81 % y 84 % del trabajo de las ventanas.
- Cuando la necesidad baja de 3 a 2 personas, la tercera ocupa un turno variable: sale o pasa a otra estación y esa hora no se paga.

### Conclusiones del notebook 04

#### Lectura del paso 4 y conclusiones

**Estaciones piloto: San Mateo (punta de la mañana) y Calle 100 (punta de la tarde).**

| | San Mateo - C.C. Unisur (07503) | Calle 100 - Marketmedios (02300) |
|---|---|---|
| Punta | mañana | tarde |
| Puesto por demanda en su punta | 8 | 1 |
| Pico | 7.452 validaciones por hora a las 6 h | 7.647 por hora a las 17 h |
| Área y densidad | 493 m²; 15,1 por hora por m² (puesto 3) | 814 m²; 9,4 por hora por m² (puesto 1) |
| Accesos | 2 activos; principal (peatonal sur) 67 % | 2 activos; norte 55 % y sur 45 % |
| Estabilidad del pico (variación entre días) | 2,3 % | 9,2 % |
| Efecto semanal | lunes -2,3 %, miércoles +1,5 % | martes +3,5 %, viernes -7,2 % |

- **Por qué estas dos:** son estaciones con flujo de portal en una fracción del espacio y con casi toda la demanda de la hora concentrada en uno o dos accesos; están entre las críticas por demanda y por presión respecto al espacio; y juntas cubren las dos puntas. San Mateo tiene el pico más estable de la lista; Calle 100 es menos estable, con los viernes claramente más bajos (el pico del viernes ronda 6.100 validaciones por hora frente a unas 8.000 los demás días).
- **Alternativas descartadas:** Terreros (más densa, 18,6 por m², pero 12.º por demanda) y Portal Américas (primero por demanda, sin m² en el archivo oficial).

**Apoyo sugerido (escenarios, no datos).** La capacidad de una persona de apoyo (validaciones por hora que puede ordenar) no está en los datos; se probaron 1.000, 1.500 y 2.000 y debe calibrarse con la operación.
- **San Mateo, 5:00 a 8:00 (3 horas):** en la hora del pico (7.452 por hora) se necesitarían **8, 5 o 4 personas** según el supuesto (1 por cada 1.000, 1.500 o 2.000); el acceso peatonal sur (67 %) concentraría 5, 4 o 3 y el norte (33 %), 3, 2 o 2.
- **Calle 100, 16:00 a 19:00 (3 horas):** en la hora del pico (7.647 por hora), **8, 6 o 4 personas**; acceso norte (55 %) 5, 3 o 3 y acceso sur (45 %) 4, 3 o 2.

**Personal de apoyo (paso 5).** Con una persona por cada 1.500 validaciones por hora (escenario), las 93 estaciones en alta presión necesitan 939 persona-horas por día; 610 caen dentro de las ventanas de la mañana y de la tarde. Se compararon tres propuestas: **A**, núcleo fijo por ventana en la misma estación (152 personas de unas 16 horas por semana); **B**, núcleo compartido entre estaciones (81 personas de 29 horas por semana, con turno partido y pausa de 7 horas); y **C**, **turnos continuos**: 48 turnos fijos de 8 horas y 219 turnos variables de 4 horas por día, con 53 personas fijas contando 10 % de relevo, que cubren toda la necesidad del día con una utilización de 75 %. A y B cubren solo las ventanas (65 %) y exigen contratos de tiempo parcial o turnos partidos; **C es la propuesta recomendada** porque usa contratos estándar y cabe en el tope de 42 horas. Cuando la necesidad baja, sale el personal variable; el núcleo no cambia.

**Qué aportó cada análisis al plan:** los **accesos** dicen *dónde dentro de la estación* ubicar a las personas y cuántas por acceso; los **m² y los accesos activos** dicen qué estaciones suben de prioridad por tener poco espacio (San Mateo, Calle 100, Virrey, Héroes) y justifican las pilotos; el **ranking por franja** dice *qué estaciones* y la **ventana crítica**, *a qué horas*; el **mapa y los perfiles** muestran en qué zonas de la ciudad ocurre cada punta (portales de la periferia en la mañana, centro y norte en la tarde).

**Variable objetivo del modelo:** `validaciones` por hora de cada estación; el notebook 05 construye los métodos de comparación y el modelo para las dos pilotos y después para todas las estaciones.

#### Conclusiones

1. Se analizan solo los **14 días hábiles**: el sábado, el domingo y el festivo son otro régimen (30 a 62 % de un día hábil y otra curva).
2. La presión se concentra en **dos puntas** (5-8 h y 16-19 h) y en **pocas estaciones** (26 de 144 reúnen la mitad de la demanda).
3. La necesidad de refuerzo se midió con la **carga sobre el umbral** (intensidad y duración); el ranking es robusto frente al volumen y al umbral. Se calculó un **ranking por franja** para no dejar la tarde tapada por la mañana.
4. **Mañana:** portales y cabeceras de la periferia, con 8 a 14 mil validaciones por hora y ventana crítica de 5:00 a 8:00 o de 6:00 a 9:00. **Tarde:** Calle 100 (7,6 mil por hora), Portal Norte, El Dorado y estaciones céntricas, con ventana de 16:00 a 19:00.
5. La **presión respecto al espacio** (m² o accesos activos) casi no cambia el top de la mañana, pero sube a estaciones pequeñas de la tarde y confirma a San Mateo y Calle 100.
6. Dentro de la estación, el **acceso principal** recibe entre 26 % y 67 % de la hora crítica; en los portales casi dos tercios del día entra por alimentación; y el cuarto de hora más cargado supera a la hora entre 4 % y 19 %.
7. Hay cuatro **perfiles de estación**; 9 de las 10 primeras del ranking son de mañana.
8. El pico de las estaciones del ranking es **estable** (variación mediana de 4,6 %), lo que permite anticiparlo.
9. El personal de apoyo se cubre con **turnos continuos**: 53 personas fijas de tiempo completo (40 horas por semana) y 219 turnos variables de 4 horas por día para las 93 estaciones en alta presión. Las propuestas por ventana (152 personas de tiempo parcial, o 81 compartidas entre estaciones) cubren solo el 65 % de la necesidad del día.

**Limitaciones:** solo 14 días hábiles en tres semanas separadas; la necesidad de personal no está en los datos, el umbral de 1.000 validaciones por hora es provisional y la capacidad por persona es un supuesto, ambos a calibrar con la operación; solo se observan entradas; los m² no existen para los portales y cabeceras; 11 de 144 estaciones sin coordenadas. No hay costos ni tipo de contrato reales: los turnos continuos suponen un recargo de 33 % para la hora variable, 10 % de relevo y una bolsa de personal que acepte turnos de 4 horas por llamado (unos 219 por día); en las propuestas por ventana, el traslado entre estaciones se estimó en línea recta y 10 parejas incluyen una estación sin coordenadas.

**Siguiente paso:** notebook 05, con el modelo de predicción de las validaciones por estación y hora comparado contra métodos simples con validación temporal, aplicado a San Mateo y Calle 100 y luego a todas.

## 5. Modelo simple (notebook 05)

Lee la tabla de AWS. Validación temporal: entrena con la semana del 10 de agosto (historia: 3 de agosto) y prueba con la del 14 de septiembre (historia: las dos de agosto). Métricas en la semana de prueba (3.576 filas; 469 de alta presión, con 1.000 validaciones por hora o más):

| Método | MAE (todas) | RMSE (todas) | R² (todas) | MAE (alta presión) | RMSE (alta presión) | Error relativo mediano (alta presión) |
|---|---|---|---|---|---|---|
| A. Semana anterior | 41,2 | 109,8 | 0,99 | 167,8 | 264,6 | 4,4 % |
| **B. Promedio histórico** | **38,1** | **104,4** | 0,99 | **152,9** | **251,1** | **3,9 %** |
| C. Regresión lineal | 42,0 | 108,0 | 0,99 | 172,2 | 260,6 | 4,7 % |
| D. Bosque aleatorio pequeño | 47,7 | 159,6 | 0,98 | 216,0 | 416,3 | 5,8 % |

Ranking de estaciones con lo predicho frente a lo real: los cuatro métodos recuperan 10 de las 10 primeras estaciones de la mañana y 8 de las 10 de la tarde (Spearman de 0,95 a 0,99). Decisión: el método de predicción es el promedio histórico por estación y hora.

### 5. Conclusiones

1. **Datos y método.** El modelo lee de AWS la tabla `analisis_estacion_hora` (10.776 filas, tres semanas) y predice la demanda de un día hábil típico por estación y hora, con validación temporal: entrena con la semana del 10 de agosto y prueba con la del 14 de septiembre.
2. **Líneas base.** Repetir la semana anterior o promediar las semanas anteriores predice con R² de 0,98 a 0,99. El promedio histórico es mejor: error absoluto medio de 153 validaciones por hora en las horas de alta presión y error relativo mediano de 3,9 %.
3. **Modelos.** Ni la regresión lineal (error de 172) ni un bosque aleatorio pequeño (216) mejoran al promedio histórico.
4. **Ranking de estaciones.** Los cuatro métodos recuperan las 10 primeras estaciones de la mañana (10 de 10) y 8 de las 10 de la tarde. Para priorizar estaciones basta una regla simple.
5. **Decisión.** El método de predicción del proyecto es el **promedio histórico por estación y hora**.

**Limitaciones**

- Solo hay tres semanas: una de entrenamiento y una de prueba. Hay una sola prueba temporal, así que no se puede medir qué tanto varía el resultado.
- Las semanas de agosto y la de septiembre no son consecutivas.
- En el entrenamiento el promedio histórico y la semana anterior coinciden (hay una sola semana de historia), por lo que los modelos no aprenden a pesarlos.
- No hay variables externas (clima, eventos, puentes festivos) ni datos del componente zonal.
- La predicción es del promedio de un día hábil, no de un día específico.

**Trabajo futuro:** a medida que el flujo cargue más semanas, repetir la prueba con varias semanas de entrenamiento y varias pruebas temporales; probar variables de calendario (festivos, puentes) y revisar las estaciones que cambian de régimen.

## 6. Dask: configuraciones de workers y memoria (punto e, notebook 01)

| método | workers | hilos | memoria por worker | minutos | veces más rápido | pico de memoria (GB) |
|---|---|---|---|---|---|---|
| pandas (1 proceso) | 1 | 1 | sin límite | 5.291 | 1.0 | 1.111 |
| Dask A | 2 | 1 | 2GB | 2.199 | 2.407 | 1.891 |
| Dask B | 4 | 1 | 2GB | 1.68 | 3.149 | 2.854 |
| Dask C | 8 | 1 | 1500MB | 1.536 | 3.446 | 4.618 |

Los cuatro métodos leen las 31.530.646 filas y generan el mismo Parquet (1,27 GB). Más workers reducen el tiempo, a costa de más memoria total.

## 7. Flujo de Prefect y base de datos en AWS (puntos d y f)

- Flujo: `src/pipeline_transmilenio.py`. Tareas: `detectar_archivos`, `validar_esquema`, `agregar_con_dask`, `semanas_pendientes`, `calcular_analisis`, `cargar_a_aws`. Parámetros: carpeta, fecha de corte, forzar, tipo de día, umbral, capacidad por persona, workers y memoria de Dask.
- Recurrente: se puede dejar programado (por ejemplo, todos los días a las 6:00); si no hay una semana completa nueva, termina sin hacer nada.
- Escalable: guarda en caché el conteo de cada día; cuando llega una semana nueva solo procesa los archivos nuevos y rehace el análisis de esa semana.
- Idempotente: carga con *upsert* por (`semana_inicio`, `cod_estacion`, `hora`) en una sola transacción, con 2 reintentos; repetir una corrida no duplica filas.
- Base: AWS RDS PostgreSQL, una sola tabla (`analisis_estacion_hora`) con 10,776 filas, solo resultados agregados (sin validaciones individuales ni tarjetas). Grafana usa un usuario de solo lectura.

| semana | filas | estaciones | días hábiles | cargada el |
|---|---|---|---|---|
| 2026-08-03 | 3528 | 147 | 4 | 2026-10-20 00:07 UTC |
| 2026-08-10 | 3624 | 151 | 5 | 2026-10-20 00:14 UTC |
| 2026-09-14 | 3624 | 151 | 5 | 2026-10-20 00:14 UTC |

Evidencias: `docs/evidencias/flujo_cargas_por_semana.md` (consulta y registros de las tres corridas), `docs/diagrama_er_analisis_estacion_hora.png` y `docs/arquitectura.png`.

## 8. Tablero de Grafana (punto g)

`grafana/dashboard.json` (se levanta con `docker compose up -d`). Filtros: semana, franja y estación. Paneles: estaciones en alta presión, pico máximo, suma de personas en cada pico y carga sobre el umbral; ranking con ventana de refuerzo; las 15 estaciones con más carga; demanda por hora de las 12 estaciones con más carga; mapa de estaciones; perfil horario de la estación elegida; pico de las cinco primeras estaciones en barras por semana. Capturas a tamaño completo: `docs/evidencias/grafana_tablero_manana_semana_14sep.png` y `docs/evidencias/grafana_tablero_tarde_semana_14sep.png`.

## 9. Limitaciones (para la sección de conclusiones del documento)

- Solo dos periodos de datos (3 a 16 de agosto y 14 a 20 de septiembre de 2026); 14 días hábiles en total y tres semanas cargadas en AWS.
- La necesidad de personal no está en los datos: se infiere de la demanda con un umbral de 1.000 validaciones por hora y una capacidad por persona de 1.500, ambos supuestos a calibrar con la operación.
- Solo se observan entradas al sistema; no hay datos del componente zonal.
- No hay costos reales ni tipo de contrato: la propuesta C supone un recargo de 33 % para la hora variable, 10 % de relevo y una bolsa de personal que acepte turnos de 4 horas por llamado.
- El modelo tiene una sola semana de entrenamiento y una de prueba, y las semanas no son consecutivas.
- Algunas estaciones no tienen coordenadas (afecta el mapa y la distancia de la propuesta B).

## 10. Mapa de evidencias del repositorio

| Punto de la rúbrica | Evidencia |
|---|---|
| a, b, c | notebooks 01, 02 y 03; `docs/decisiones_limpieza.csv` |
| d | `src/pipeline_transmilenio.py`; `docs/evidencias/prefect_*.jpg`; `docs/evidencias/flujo_cargas_por_semana.md` |
| e | notebook 01; `docs/benchmark_dask.csv` |
| f | `docs/diagrama_er_analisis_estacion_hora.png`; `docs/evidencias/flujo_cargas_por_semana.md`; captura de DBeaver (del equipo) |
| g | `grafana/dashboard.json`; `docs/evidencias/grafana_tablero_*.png` |
| h | notebooks 03, 04 y 05 (conclusiones); esta ficha |
| l | repositorio de GitHub |
