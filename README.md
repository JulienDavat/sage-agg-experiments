# sage-sparql-void
Execution of VoID description over Sage endpoints

## Install 

Using gradle distZip
```bash
git clone https://github.com/folkvir/sage-sparql-void.git
cd sage-sparql-void
gradle clean distZip
cd build/distributions/
unzip sage-sparql-void.zip
cd sage-sparql-void
./bin/sage-sparql-void
```
Using gradle fatJar
```bash
git clone https://github.com/folkvir/sage-sparql-void.git
cd sage-sparql-void
gradle clean fatjar
java -jar build/libs/sage-sparql-void-fat-1.0.jar
```

Run sage server:
```bash
docker pull callidon/sage
docker run -d -v $(pwd)/data/:/opt/data/ -p 8000:8000 callidon/sage sage /opt/data/void.yaml -w 4 -p 8000
```


Wikidata 8B Sage 75 ms 2000 results
```bash
cp sage-sparql-void/data/sage-configs/*.yaml ./datasets/
# 2018 (8B)
docker run -d --name wikidata8b -v $(pwd)/datasets/:/opt/data/ -p 7780:7780 callidon/sage sage /opt/data/wikidata8b150ms.yaml -w 4 -p 7780
# 2017 (2B)
docker run -d --name wikidata2b -v $(pwd)/datasets/:/opt/data/ -p 7720:7720 callidon/sage sage /opt/data/wikidata2b150ms.yaml -w 4 -p 7720
``` 
````bash
# 2018 (8B)
java -Xms50g -Xmx50g -jar build/libs/sage-sparql-void-fat-1.0.jar dataset http://172.16.8.50:7780/sparql/wikidata http://172.16.8.50:7780/sparql/wikidata ./output/
# 2017 (2B)
java -Xms50g -Xmx50g -jar build/libs/sage-sparql-void-fat-1.0.jar dataset http://172.16.8.50:7720/sparql/wikidata http://172.16.8.50:7720/sparql/wikidata ./output/
````