import boto3
import uuid
import os
import json
from datetime import datetime, timezone

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

def _parse_body(event):
    # Si viene como string (proxy), parsea; si ya es dict, úsalo tal cual
    body = event.get('body')
    if body is None:
        return event  # fallback: a veces mapean todo al root
    if isinstance(body, str):
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            # Algunos mapeos no-proxy envían key=value; ajusta si fuera tu caso
            raise ValueError("El body no es un JSON válido")
    return body  # ya era dict

def lambda_handler(event, context):
    print("Evento recibido:", event)

    body = _parse_body(event)
    tenant_id = body['tenant_id']
    texto = body['texto']

    table_name = os.environ["TABLE_NAME"]
    ingest_bucket = os.environ["INGEST_BUCKET"]

    # Construye el comentario (igual que antes)
    uuidv1 = str(uuid.uuid1())
    comentario = {
        'tenant_id': tenant_id,
        'uuid': uuidv1,
        'detalle': {
            'texto': texto
        },
        'ts': datetime.now(timezone.utc).isoformat()
    }

    # 1) Guarda en DynamoDB (como ya lo hacías)
    table = dynamodb.Table(table_name)
    response = table.put_item(Item=comentario)

    # 2) Ingesta Push a S3 (JSON pretty, UTF-8)
    key = f"{tenant_id}/{uuidv1}.json"
    s3.put_object(
        Bucket=ingest_bucket,
        Key=key,
        Body=json.dumps(comentario, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json; charset=utf-8"
    )

    print("Comentario guardado en DDB y en S3:", key)

    return {
        'statusCode': 200,
        'comentario': comentario,
        's3_key': key,
        's3_bucket': ingest_bucket,
        'response': response
    }
