import pandas as pd
import requests
from datetime import datetime, timedelta, timezone
from tqdm import tqdm
import time
import os

# ========== CONFIGURAÇÕES (via ENV) ==========
ACCOUNT = os.getenv("VTEX_ACCOUNT_NAME", "senffnet")
ENV = "vtexcommercestable"
APP_KEY = os.getenv("VTEX_APP_KEY")
APP_TOKEN = os.getenv("VTEX_APP_TOKEN")

OUTPUT_PATH = "output/pedidos_gerais.csv"

headers = {
    "X-VTEX-API-AppKey": APP_KEY,
    "X-VTEX-API-AppToken": APP_TOKEN,
    "Content-Type": "application/json"
}

# ========== GERAR INTERVALO DE DATAS ==========
def gerar_intervalo():
    agora = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=-3)))
    inicio = (agora - timedelta(days=4)).replace(hour=0, minute=0, second=0, microsecond=0)
    fim = agora.replace(hour=23, minute=59, second=59, microsecond=0)
    return [(inicio, fim)]

# ========== EXTRAI CAMPOS DE LISTAS ==========
def extrair_valores_lista(dados, coluna, chave_id='id', chave_valor='value', prefixo=None):
    valores = {}
    for item in dados.get(coluna, []):
        if isinstance(item, dict) and chave_id in item and chave_valor in item:
            nome_coluna = f"{prefixo or coluna}_{item[chave_id]}"
            valores[nome_coluna] = item[chave_valor]
    return valores

# ========== COLETA DE PEDIDOS ==========
def coletar_pedidos(data_inicio, data_fim):
    pedidos = []
    pagina = 1

    while True:
        url = (
            f"https://{ACCOUNT}.{ENV}.com.br/api/oms/pvt/orders?"
            f"f_creationDate=creationDate:[{data_inicio} TO {data_fim}]"
            f"&per_page=50&page={pagina}"
        )

        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code != 200:
            print(f"❌ Erro página {pagina}: {r.status_code} - {r.text}")
            break

        lista = r.json().get("list", [])
        if not lista:
            print(f"✅ Página {pagina} vazia — encerrando.")
            break

        print(f"📄 Página {pagina}: {len(lista)} pedidos")

        pedidos_validos_pagina = []

        for pedido_resumo in tqdm(lista, desc=f"Detalhes página {pagina}"):
            order_id = pedido_resumo.get("orderId")
            if not order_id:
                continue

            url_detalhe = f"https://{ACCOUNT}.{ENV}.com.br/api/oms/pvt/orders/{order_id}"

            pedido = None
            for tentativa in range(3):
                try:
                    r_det = requests.get(url_detalhe, headers=headers, timeout=30)
                    if r_det.status_code == 200:
                        pedido = r_det.json()
                        break
                except requests.exceptions.RequestException:
                    time.sleep(2)

            if not pedido:
                continue

            if pedido.get("status") == "canceled":
                continue

            # Marketing / UTM
            marketing_data = pedido.get("marketingData") or {}
            pedido["utmSource"] = marketing_data.get("utmSource")
            pedido["utmMedium"] = marketing_data.get("utmMedium")
            pedido["utmCampaign"] = marketing_data.get("utmCampaign")

            # Totals
            pedido.update(extrair_valores_lista(pedido, "totals", prefixo="totals"))

            # Seller principal
            sellers = pedido.get("sellers", [])
            pedido["sellerName"] = sellers[0].get("name") if sellers else None

            pedido["data_extracao"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            pedidos.append(pedido)
            pedidos_validos_pagina.append(pedido)

        if not pedidos_validos_pagina:
            print(f"✅ Nenhum pedido válido na página {pagina} — encerrando.")
            break

        pagina += 1
        time.sleep(0.3)

    return pedidos

# ========== MAIN ==========
def main():
    os.makedirs("output", exist_ok=True)

    intervalos = gerar_intervalo()
    todos_pedidos = []

    for inicio, fim in intervalos:
        i_utc = inicio.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        f_utc = fim.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        todos_pedidos.extend(coletar_pedidos(i_utc, f_utc))

    print(f"Total de pedidos coletados: {len(todos_pedidos)}")

    df = pd.json_normalize(todos_pedidos, sep="_")
    df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")

    print(f"CSV gerado: {OUTPUT_PATH} ({len(df)} linhas)")
