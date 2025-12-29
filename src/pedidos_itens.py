import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from tqdm import tqdm
import time
import os

# ========== CONFIGURAÇÕES (ENV) ==========
ACCOUNT = os.getenv("VTEX_ACCOUNT_NAME", "senffnet")
ENV = "vtexcommercestable"
APP_KEY = os.getenv("VTEX_APP_KEY")
APP_TOKEN = os.getenv("VTEX_APP_TOKEN")

OUTPUT_PATH = "output/pedidos_itens.csv"

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
    return inicio, fim

# ========== COLETA DE ITENS ==========
def coletar_itens():
    itens = []
    pagina = 1

    inicio, fim = gerar_intervalo()
    i_utc = inicio.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    f_utc = fim.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    while True:
        url = (
            f"https://{ACCOUNT}.{ENV}.com.br/api/oms/pvt/orders?"
            f"f_creationDate=creationDate:[{i_utc} TO {f_utc}]"
            f"&per_page=50&page={pagina}"
        )

        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code != 200:
            print(f"❌ Erro página {pagina}: {r.status_code}")
            break

        pedidos = r.json().get("list", [])
        if not pedidos:
            print(f"✅ Página {pagina} vazia — encerrando.")
            break

        for resumo in tqdm(pedidos, desc=f"Página {pagina}"):
            order_id = resumo.get("orderId")
            if not order_id:
                continue

            url_det = f"https://{ACCOUNT}.{ENV}.com.br/api/oms/pvt/orders/{order_id}"

            pedido = None
            for tentativa in range(3):
                try:
                    r_det = requests.get(url_det, headers=headers, timeout=30)
                    if r_det.status_code == 200:
                        pedido = r_det.json()
                        break
                except requests.exceptions.RequestException:
                    time.sleep(2)

            if not pedido:
                continue

            for item in pedido.get("items", []):
                item["orderId"] = order_id
                item["data_extracao"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                itens.append(item)

        pagina += 1
        time.sleep(0.3)

    return itens

# ========== MAIN ==========
def main():
    os.makedirs("output", exist_ok=True)

    dados = coletar_itens()

    if dados:
        df = pd.json_normalize(dados, sep="_")
        df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")
        print(f"✅ CSV de itens gerado: {OUTPUT_PATH} ({len(df)} linhas)")
    else:
        print("⚠️ Nenhum item encontrado.")

if __name__ == "__main__":
    main()
