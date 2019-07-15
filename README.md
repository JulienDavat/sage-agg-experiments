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
```
docker pull callidon/sage
docker run -d -v $(pwd)/data/:/opt/data/ -p 8000:8000 callidon/sage sage /opt/data/void.yaml -w 4 -p 8000
```


Wikidata 8B Sage 75 ms 2000 results
```
cp ./sage-configs/wikidata.yaml ./datasets/
docker run -d --name wikidatasage -v ./datasets/:/opt/data/ -p 8888:8888 callidon/sage sage /opt/data/wikidata.yaml -w 4 -p 8888
``` 