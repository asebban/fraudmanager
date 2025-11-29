Build : mvn clean package (génère un JAR avec dépendances intégrées).
Lancer Redis : Utilisez redis-server /path/to/redis.conf pour activer la persistance.
Lancer NATS : Utilisez un serveur NATS standard.
Exécuter l'app : java -jar target/fraud-detection-java-1.0-SNAPSHOT-jar-with-dependencies.jar.
Tests : Ajoutez des tests unitaires pour chaque service (ex. : mock Redis avec Testcontainers).
Monitoring : Intégrez Micrometer/Prometheus pour TPS et latences.
Scalabilité : Si besoin, shardez Redis ou ajoutez plus de threads/instances Java.

To deploy:

Set rocksdb.config.file in application.properties
Create this central config file with shard assignments
Start each node with --node.name node-X argument
Verify shard directories are created correctly
Monitor logs for shard routing messages