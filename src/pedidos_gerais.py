import pandas as pd
import requests
from datetime import datetime, timedelta, timezone
from tqdm import tqdm
import time
import os

# ========== CONFIGURAÇÕES (via ENV) ==========
ACCOUNT = os.getenv("VTEX_ACCOUNT_NAME")
ENV = "vtexcommercestable"
APP_KEY = os.getenv("VTEX_APP_KEY")
APP_TOKEN = os.getenv("VTEX_APP_TOKEN")

OUTPUT_PATH = "output/pedidos_gerais.csv"

headers = {
    "X-VTEX-API-AppKey": APP_KEY,
    "X-VTEX-API-AppToken": APP_TOKEN,
    "Content-Type": "application/json"
}

# ========== FUSO HORÁRIO ==========
TZ_BR = timezone(timedelta(hours=-3))

# ========== GERAR INTERVALO (ONTEM – BRASÍLIA) ==========
def gerar_intervalo():
    agora_br = datetime.now(TZ_BR)

    inicio_br = (agora_br - timedelta(days=6)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    fim_br = agora_br.replace(
        hour=23, minute=59, second=59, microsecond=0
    )

    return [(inicio_br, fim_br)]

# ========== COLETA DE PEDIDOS ==========
def coletar_pedidos(data_inicio_utc, data_fim_utc):
    pedidos = []
    pagina = 1

    while True:
        url = (
            f"https://{ACCOUNT}.{ENV}.com.br/api/oms/pvt/orders?"
            f"f_creationDate=creationDate:[{data_inicio_utc} TO {data_fim_utc}]"
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

        pedidos_validos = []

        for pedido_resumo in tqdm(lista, desc=f"Detalhes página {pagina}"):
            order_id = pedido_resumo.get("orderId")
            if not order_id:
                continue

            url_detalhe = f"https://{ACCOUNT}.{ENV}.com.br/api/oms/pvt/orders/{order_id}"

            pedido = None
            for _ in range(3):
                try:
                    r_det = requests.get(url_detalhe, headers=headers, timeout=30)
                    if r_det.status_code == 200:
                        pedido = r_det.json()
                        break
                except requests.exceptions.RequestException:
                    time.sleep(2)

            if not pedido or pedido.get("status") == "canceled":
                continue

            # -------- MARKETING / UTM --------
            marketing = pedido.get("marketingData") or {}
            pedido["utmSource"] = marketing.get("utmSource")
            pedido["utmMedium"] = marketing.get("utmMedium")
            pedido["utmCampaign"] = marketing.get("utmCampaign")

            # -------- SELLER PRINCIPAL --------
            sellers = pedido.get("sellers", [])
            pedido["sellerName"] = sellers[0].get("name") if sellers else None

            # -------- TOTALS NORMALIZADOS --------
            for total in pedido.get("totals", []):
                if isinstance(total, dict):
                    col = f"totals_{total.get('id')}"
                    pedido[col] = total.get("value")

            # -------- DATA DE EXTRAÇÃO --------
            pedido["data_extracao"] = datetime.now(TZ_BR).strftime(
                "%Y-%m-%d %H:%M:%S"
            )

            pedidos.append(pedido)
            pedidos_validos.append(pedido)

        if not pedidos_validos:
            print(f"✅ Nenhum pedido válido na página {pagina} — encerrando.")
            break

        pagina += 1
        time.sleep(0.3)

    return pedidos

# ========== MAIN ==========
def main():
    os.makedirs("output", exist_ok=True)

    todos_pedidos = []

    for inicio_br, fim_br in gerar_intervalo():
        inicio_utc = inicio_br.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        fim_utc = fim_br.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        print(f"🔎 Coletando pedidos de {inicio_br} até {fim_br} (Brasília)")
        todos_pedidos.extend(coletar_pedidos(inicio_utc, fim_utc))

    if not todos_pedidos:
        print("⚠️ Nenhum pedido encontrado.")
        return

    df = pd.json_normalize(todos_pedidos, sep="_")

    # -------- COLUNAS DO RELATÓRIO --------
    colunas_relatorio = [
        "orderId",
        "creationDate",
        "value",
        "sellerName",
        "statusDescription",
        "utmSource",
        "utmMedium",
        "utmCampaign",
        "email",
        "totals_Items",
        "totals_Discounts",
        "totals_Shipping",
        "totals_Tax",
        "data_extracao"
    ]

    # Garante que todas existam
    for col in colunas_relatorio:
        if col not in df.columns:
            df[col] = None

    df = df[colunas_relatorio]

    df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")
    print(f"✅ CSV gerado: {OUTPUT_PATH} ({len(df)} linhas)")

if __name__ == "__main__":
    main()
