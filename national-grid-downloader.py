import json
import requests
import signal
import sys
import os
from dotenv import load_dotenv

load_dotenv()

def signal_handler(sig, frame):
    print('Canceling downloads')
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# Obtained through jQuery selector on National Grid's data portal
urls = [
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/25390cdc-4844-4360-ae89-40fb0626c24b/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0001.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/55fed396-0019-4262-824c-7de85b93e05c/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0000.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/41a8b24f-18dd-4639-954c-295bb766c064/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0003.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/ba004964-3e80-4925-b081-7efa03c7deaa/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0002.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/9de33376-932e-4b74-bd4e-472396d74a02/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0005.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/cda379f8-6a9a-498b-a4f5-d53a2a75acb4/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0006.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/6cb1b81d-23c9-4f4b-884f-fb1b182bd599/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0007.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/824264ac-3aec-4733-a45b-b402b84de38b/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0009.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/4f455f6f-9f9c-44a9-87e6-3684aaf25fc0/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0004.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/c149e59e-36c0-4657-933b-e229462ede25/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0008.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/13c6f9eb-de5f-4af8-aaaf-7f36ac26adf0/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0012.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/d0621d1e-dcc9-4a7e-a5fe-6845d7a0c45f/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0011.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/3fcaaeef-7ac5-414a-82d8-e766bb6f24ad/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0014.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/56afe525-b9f8-441b-a8de-09922d48acc9/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0010.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/ba859f5c-d549-402b-ac7f-35911580fd42/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0013.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/dfb992bc-68d3-4181-8ff6-1e6d6281f8fe/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0017.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/e2d3ea41-e671-4416-8e81-50d709df4a2b/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0016.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/15b66459-d36a-481c-a483-4590bee06f5b/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0015.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/815657e2-f9c0-4da7-8774-e1fcf55ad228/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0020.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/aa627ee7-bffe-4217-894f-63bcb5ea0d5c/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0018.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/3817f409-a4db-48cd-9bee-e2a7ff5dae15/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0019.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/9c73d5fb-f396-42a2-a81b-e87c7b55fe40/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0021.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/10c0585e-e229-4a43-87c3-2f0650230edf/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0022.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/016c6ead-39cf-4971-88bc-fcc5b10204b8/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0025.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/e095e90c-f009-4536-b305-80b7c337c179/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0024.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/17e53360-df15-460c-a866-57df7167413c/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0023.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/a43e50c3-8851-4d1d-aea6-72696f6e11d7/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0026.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/bbcd98c3-e35e-4de8-b503-7f0dab795b0e/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0027.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/9b31ab07-cc60-41aa-92f9-2f51bc964ef3/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0028.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/268ae2a6-53bd-4857-8a48-a427992d5913/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0030.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/a80d1412-8e70-4266-9078-7f4994f88b94/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0031.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/4e5eb504-5d3e-42dd-b6ec-13be68235329/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0032.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/639bcf02-eae3-4256-990c-58e27c964552/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0029.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/2ff0f88b-2849-4503-b381-dc56f0432be4/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0035.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/a3f67203-d497-4838-a7a3-e3017dcae073/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0033.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/2944e315-9eff-4a20-b9cd-85629fa46e71/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0034.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/950acd5a-7665-4b53-a617-0a197db9169c/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0036.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/323db86f-ed82-4a8d-903f-ea47528eb01a/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0038.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/91bd3ffc-9280-4602-be6e-1685b14cf49b/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0039.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/03a1eb3d-5d1a-4e2d-9940-ddcad45e1914/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0037.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/a00e17f9-2975-44e8-8dde-23444ab4dee1/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0042.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/d70ae209-d300-4798-8ca7-af9b89e486ad/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0041.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/dd696171-f096-4818-8a3a-6f5ce2350aae/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0040.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/e36bffff-6b0a-4eb8-b4ed-38bde7e96d5e/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0044.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/910f19be-b9da-4637-8fcc-689ace4272a0/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0043.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/4bc7ebd7-45e2-400f-bfa1-1de3f44bad60/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0046.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/eba65bb6-6ba7-4548-b4d9-c6eccb03a2da/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0047.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/d151ca6f-e130-4b7c-b01a-bf367582ed91/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0045.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/e5fc2f6b-8d81-41a5-9539-25c932804af9/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0049.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/61f7ea78-4081-497b-bd04-d2d80df6cfe7/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0050.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/9bf2b7f5-6a06-4eaa-8109-23b0becf4870/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0051.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/0642e963-b734-4d4e-845e-73642fe7c8b1/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0048.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/344fccf1-76a2-402c-84c9-d610d654a4ed/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0053.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/210a72a4-176b-48e4-a661-52ff74d7fb2a/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0052.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/d3461ad6-ec74-4f9a-8a65-1edbb79e80ac/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0054.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/30ea2699-7c08-4ad7-bdc6-58e9ceb0c900/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0056.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/8702f71a-a044-443a-8872-4a371702ae83/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0058.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/cfb9662b-de54-472b-8c5b-3376148487d1/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0057.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/c7e35c25-c056-4e96-a993-80ccb093833d/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0055.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/efd0c5ad-db43-4327-b6e9-5a7747317031/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0060.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/96acd12b-758d-4365-a200-b5d7a8b4abd6/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0061.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/6d4c00f9-d6df-434d-8fa3-2247f22756a5/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0059.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/e9fb57a4-1efb-47e0-982b-be0987e0b125/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0062.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/634aa2e9-bd09-438f-af57-bf94a069725e/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0064.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/b8fc5398-26ea-45db-a808-dd6e27790e64/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0065.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/10061278-d264-44ba-92cb-55286340aa95/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0063.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/fa448737-52be-4445-8110-fa2784c5657d/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0068.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/d7a441c0-80fe-4795-a423-1ce8215a57ec/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0067.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/df00cc0e-6239-4420-87f0-143646bc15e5/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0066.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/575dbb90-7cc4-463c-b47f-0f1eb8d4bb37/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0069.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/632c8986-1261-4f9e-b94d-bb7e929aa7a9/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0070.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/3f32e875-725d-4a1a-8df9-727dc0880b4c/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0071.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/034ab50a-dfe7-47df-a742-84c57fef847c/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0072.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/dfb1fc8f-97c4-4bb2-baef-cba07cdd2c56/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0073.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/4aca589e-07f3-454f-abcd-c0718a5875ef/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0075.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/5c23f159-458e-43ed-936b-844e41776930/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0076.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/ac2cfab1-568f-4d1f-8d14-0ebfccb4df0e/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0077.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/da7b28fd-9a3a-4185-bbdd-5a8ff3ce15a8/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0078.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/58d4678e-dd8d-42a4-8b5c-1955ed56dd58/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0074.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/2b0b7a8f-4b16-4d5c-8ce3-9bdac336fd8f/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0080.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/94e05978-8a36-4790-ba03-2c69c894ef17/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0081.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/a30d4151-9afc-48b5-96c4-737e93200a3d/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0082.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/b78db71d-ec3c-4579-b71b-c4ecfe850b36/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0084.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/460a2937-da4e-4194-950b-a9e9303310cc/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0083.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/31e32976-c08b-41b7-acde-11392db00bd2/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0085.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/f71fb281-1cff-4107-a492-ee2b4f1d975d/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0079.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/f87b45a9-e074-4a16-a81d-d9a38c6d457b/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0086.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/77b32977-18a3-44c2-a841-e534c754e01a/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0088.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/ffec7789-0b5d-43fa-ba7b-327b60ac1d9e/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0089.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/e5c405c9-499f-440d-8bb2-a7b51599c0ec/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0090.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/2dcb489b-60d9-4b66-8c5e-5c2888d5d154/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0091.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/c46a57eb-60fb-4db8-8dad-f374bfd295f3/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0087.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/baadbaf2-3480-4b8f-ab2f-ad291aad531c/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0092.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/13831638-0f43-461f-96f8-e2d75b69a0f4/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0093.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/9ffadeff-8165-4ff5-aa59-73b01b57a228/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0095.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/176408ff-a7aa-4746-88e6-7a5ec33067e0/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0094.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/2165f28a-bc4f-4f97-b595-53cac33ac784/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0097.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/af4f66a4-0275-4e40-8081-697f2d2b9361/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0096.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/3a2dcf66-9f38-475b-8f4c-e53fa9051a9e/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0098.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/37f26f30-eb49-43af-a3f8-5d748ef5a33f/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0099.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/e01bb1f2-2d2f-476f-8c61-25168df13b41/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0100.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/15a55a7a-e247-4bfc-9487-58b552756e37/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0102.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/7400771e-2002-4e9b-bcbc-add5b932f435/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0103.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/b06692a2-8355-4fe5-b9c1-0deccc9112a0/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0101.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/4e260f8f-e794-4804-96a4-1db421b38bf3/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0105.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/b03e5de3-df73-46be-ab4c-806dff5a70b2/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0107.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/8da565fd-4d12-47b0-9e8d-5792df9a33a5/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0104.csv",
    "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/3961e521-ca56-421c-82ad-7a23a90ee8b4/download/aggregated-smart-meter-data-lv-feeder-2024-02-part0106.csv"
]

print(f"Downloading {len(urls)} files from National Grid")

api_token = os.getenv("NATIONAL_GRID_API_TOKEN")

with requests.Session() as s:
    s.headers.update({'Authorization': api_token})
    for url in urls:
        filename = url.split('/')[-1]
        output = f"data/raw/nged/{filename}"

        # skip if output file exists
        if os.path.exists(output):
            print(f"Skipping {url} as {output} already exists")
            continue
        else:
            print(f"Downloading {url} into {output}")

        with s.get(url, stream=True) as r:
            r.raise_for_status()
            with open(output, 'wb') as f:
                total_size = int(r.headers.get('content-length', 0))
                downloaded_size = 0
                for chunk in r.iter_content(chunk_size=8192):
                    downloaded_size += len(chunk)
                    progress = int(downloaded_size / total_size * 100)
                    print(f"Downloading... {progress}% complete", end="\r")
                    f.write(chunk)
