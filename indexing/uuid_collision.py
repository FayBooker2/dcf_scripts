import asyncio
import aiohttp
import csv

# Open tsv and txt files(open txt file in write mode)
tsv_file = open("/Users/faybooker/Downloads/CTDC/File_Transfer_Manifest_for_ICDC_Test_Files_Destined_for_CTDC_v1_indexd.tsv")
txt_file = open("/Users/faybooker/Downloads/CTDC/collisions.txt", "w")

read_tsv = csv.reader(tsv_file, delimiter="\t")

# write data in txt file line by line
total = 0
urls = []
for row in read_tsv:
    url = 'https://nci-crdc-staging.datacommons.io/index/index/' + row[0]
    urls.append(url)
    total += 1

# Initialize connection pool
conn = aiohttp.TCPConnector(limit_per_host=100, limit=0, ttl_dns_cache=300)
PARALLEL_REQUESTS = 100
results = {}

async def gather_with_concurrency(n):
    semaphore = asyncio.Semaphore(n)
    session = aiohttp.ClientSession(connector=conn)

    # heres the logic for the generator
    async def get(u, urls):
        url = urls[u]
        async with semaphore:
            async with session.get(url, ssl=False) as response:
                print(((u) / total) * 100, url)
                if response.status not in results:
                    results[response.status] = [url]
                else:
                    results[response.status] += [url]
    await asyncio.gather(*(get(u, urls) for u in range(len(urls))))
    await session.close()

loop = asyncio.get_event_loop()
loop.run_until_complete(gather_with_concurrency(PARALLEL_REQUESTS))
conn.close()

if 200 in results:
    for c in results[200]:
        txt_file.writelines(c + '\n')
else:
    print("No Collisions Found!")
    for key, value in results.items():
        print(key)
    txt_file.writelines(str(results))

print(f"Completed {len(urls)} requests with {len(results)} results")
