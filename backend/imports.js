const fs = require('fs');
const { MongoClient } = require('mongodb');
const Redis = require('ioredis');

async function importData() {
    // Configuración de conexiones
    const mongoClient = new MongoClient(process.env.MONGO_URI || 'mongodb://localhost:27017');
    const redisGeo = new Redis({ host: process.env.REDIS_GEO_HOST || 'localhost', port: 6379 });
    const redisPop = new Redis({ host: process.env.REDIS_POP_HOST || 'localhost', port: 6380 });

    try {
        await mongoClient.connect();
        const db = mongoClient.db('airport_db');
        const collection = db.collection('airports');

        // Verificar si ya hay datos
        const count = await collection.countDocuments();
        if (count > 0) {
            console.log("Los datos ya están cargados.");
            return;
        }

        const rawData = fs.readFileSync('./airports.json');
        const airports = JSON.parse(rawData);


        //Preparar carga en MongoDB (Bulk Insert)
        await collection.insertMany(airports);
        console.log("MongoDB: Datos guardados.");

        //Preparar carga en Redis GEO
        const geoPipeline = redisGeo.pipeline();
        
        airports.forEach(airport => {
        const memberId = airport.iata_faa || airport.icao;

            if (airport.lng && airport.lat && memberId) {
        
            // Guardamos en Redis usando ese ID único
            geoPipeline.geoadd('airports_geo', airport.lng, airport.lat, memberId);
        
            }
        });
        await geoPipeline.exec();

        // Inicializar Redis Popularidad
        await redisPop.set('airport_popularity_initialized', 'true', 'EX', 86400);
       

        } catch (error) {
        console.error("Error en la importación:", error);
        } finally {
        await mongoClient.close();
        redisGeo.disconnect();
        redisPop.disconnect();
    }
}

module.exports = importData;