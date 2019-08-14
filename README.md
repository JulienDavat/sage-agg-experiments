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

Run sage server using a custom GUnicorn configuration
```bash
#
cd sage-engine
git submodule init
git submodule update
cd ..
```
Then:
```bash
docker-compose up -d
```

```bash
nohup java -Xms50g -Xmx50g -jar build/libs/sage-sparql-void-fat-1.0.jar dataset http://172.16.8.50:7120/sparql/wikidata http://172.16.8.50:7120/sparql/wikidata ./output/ &
nohup java -Xms50g -Xmx50g -jar build/libs/sage-sparql-void-fat-1.0.jar dataset http://172.16.8.50:7180/sparql/wikidata http://172.16.8.50:7180/sparql/wikidata ./output/ &
```

````bash
# 2018 (8B)
java -Xms50g -Xmx50g -jar build/libs/sage-sparql-void-fat-1.0.jar dataset http://172.16.8.50:7120/sparql/wikidata http://172.16.8.50:7780/sparql/wikidata ./output/
# 2017 (2B)
java -Xms50g -Xmx50g -jar build/libs/sage-sparql-void-fat-1.0.jar dataset http://172.16.8.50:7180/sparql/wikidata http://172.16.8.50:7720/sparql/wikidata ./output/
````
