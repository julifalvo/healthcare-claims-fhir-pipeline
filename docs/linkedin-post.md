# LinkedIn post drafts

Two versions of the same post, for sharing this project.

## Español

Armé un pipeline de datos end-to-end simulando el mundo de facturación médica en EE.UU.

El problema: un sistema de salud recibe sus reclamos médicos en FHIR (el estándar de
interoperabilidad clínica) y necesita responder todos los días:

- ¿Qué aseguradoras nos rechazan pagos, y por qué?
- ¿Cuánta plata rechazada podemos recuperar, y qué reclamos apelar primero?
- ¿Dónde está trabada la plata que todavía no cobramos?
- ¿Qué pacientes vuelven a internarse antes de 30 días?

Construí el pipeline completo que toma esos datos crudos, los valida, los transforma y deja
listas las tablas para responder esas preguntas sin intervención manual.

Stack: Apache Airflow + PySpark + Delta Lake (arquitectura bronze/silver/gold), con un
dashboard en Streamlit para el equipo de facturación.

Algunas decisiones de diseño que me interesó resolver:

→ Los datos con errores no se descartan: quedan en cuarentena con el motivo exacto, para
corregir y reprocesar sin perder nada.

→ Si la calidad de los datos cae, el pipeline se frena solo antes de publicar números
incorrectos a nadie.

→ Las respuestas de las aseguradoras llegan semanas después del reclamo original — el
sistema las va incorporando automáticamente a medida que aparecen.

Repo con documentación, diagramas y video de una corrida real:
github.com/julifalvo/healthcare-claims-fhir-pipeline

Datos 100% sintéticos — proyecto de portfolio para mostrar cómo diseñaría este tipo de
sistema en un contexto real.

## English

Built an end-to-end data pipeline simulating medical billing (revenue cycle) in the US
healthcare system.

The problem: a health system receives its medical claims in FHIR (the clinical
interoperability standard) and needs to answer, every day:

- Which payers are denying our claims, and why?
- How much of that denied revenue can we actually recover, and which claims should we
  appeal first?
- Where is our cash stuck?
- Which patients are coming back within 30 days of discharge?

I built the pipeline that takes that raw data, validates it, transforms it, and leaves the
tables ready to answer those questions without anyone touching it by hand.

Stack: Apache Airflow + PySpark + Delta Lake (bronze/silver/gold architecture), with a
Streamlit dashboard for the billing team.

A few design decisions I cared about getting right:

→ Bad records aren't dropped. They get quarantined with the exact reason they failed, so
they can be fixed and reprocessed without losing anything.

→ If data quality drops below a threshold, the pipeline stops itself before publishing
wrong numbers to anyone.

→ Payer responses show up weeks after the original claim — the system picks them up
automatically as they arrive.

Repo with docs, diagrams and a recording of a real run:
github.com/julifalvo/healthcare-claims-fhir-pipeline

100% synthetic data — a portfolio project to show how I'd approach this kind of system in
production.
