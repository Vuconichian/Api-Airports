const cors = require('cors');

const express = require('express');
const { MongoClient } = require('mongodb');
const Redis = require('ioredis');
const importData = require('./imports');

const app = express();
app.use(cors());
app.use(express.json());


const mongoUri = process.env.MONGO_URI || 'mongodb://localhost:27017';
const redisGeoHost = process.env.REDIS_GEO_HOST || 'localhost';
const redisPopHost = process.env.REDIS_POP_HOST || 'localhost';

const redisGeo = new Redis({ host: redisGeoHost, port: 6379 });
const redisPop = new Redis({ host: redisPopHost, port: 6379 });


async function startServer() {
    try {
        // carga inicial de datos
        await importData();
        
        // GET /airports - Devuelve todos los aeropuertos
        app.get('/airports', async (req, res) => {
            const client = new MongoClient(mongoUri);
            await client.connect();
            const airports = await client.db('airport_db').collection('airports').find().toArray();
            await client.close();
            res.json(airports);
        });

        app.post('/airports', async (req, res) => {
            const newAirport = req.body; 
            const client = new MongoClient(mongoUri);

            try {
                await client.connect();
                const db = client.db('airport_db');

                await db.collection('airports').insertOne(newAirport);

                if (newAirport.lng && newAirport.lat && (newAirport.iata_faa || newAirport.icao)) {
                    const memberId = newAirport.iata_faa || newAirport.icao;
                    await redisGeo.geoadd('airports_geo', newAirport.lng, newAirport.lat, memberId);
                }

                res.status(201).json({ message: "Aeropuerto creado con éxito", airport: newAirport });
            } catch (error) {
                res.status(500).json({ error: error.message });
            } finally {
                await client.close();
            }
        });
        
        app.put('/airports/:iata_code', async (req, res) => {
            const code = req.params.iata_code.toUpperCase();
            const updateData = req.body;
            const client = new MongoClient(mongoUri);

            try {
                await client.connect();
                const db = client.db('airport_db');

                const result = await db.collection('airports').updateOne(
                    { $or: [{ iata_faa: code }, { icao: code }] }, // Más flexible
                    { $set: updateData }
                );

                if (result.matchedCount > 0) {
                    if (updateData.lat && updateData.lng) {
                        await redisGeo.geoadd('airports_geo', updateData.lng, updateData.lat, code);
                    }

            
                    res.json({ message: "Aeropuerto actualizado correctamente en Mongo y Redis" });
                } else {
                    res.status(404).json({ error: "Aeropuerto no encontrado" });
                }
            } catch (error) {
                res.status(500).json({ error: error.message });
            } finally {
                await client.close();
            }
        });
       
        app.delete('/airports/:iata_code', async (req, res) => {
            const code = req.params.iata_code.toUpperCase();
            const client = new MongoClient(mongoUri);

            try {
                await client.connect();
                const db = client.db('airport_db');

                const result = await db.collection('airports').deleteOne({ 
                    $or: [
                        { iata_faa: code }, 
                        { iata_code: code }, 
                        { icao: code }
                    ] 
                });

                await redisGeo.zrem('airports_geo', code);
                await redisPop.zrem('airport_popularity', code);

                if (result.deletedCount > 0) {
                    res.json({ message: "Eliminado de Mongo y Redis" });
                } else {
                    res.status(404).json({ error: "Se borró de Redis pero no se encontró en Mongo" });
                }
            } catch (error) {
                res.status(500).json({ error: error.message });
            } finally {
                await client.close();
            }
        });
        // GET /airports/popular Top 10 
        app.get('/airports/popular', async (req, res) => {
            try {
                // zrevrange trae los aeropuertos y sus puntajes.
                const result = await redisPop.zrevrange('airport_popularity', 0, 9, 'WITHSCORES');

                if (!result || result.length === 0) {
                    return res.json({ message: "Todavía no hay aeropuertos populares.", ranking: [] });
                }

                const ranking = [];
                for (let i = 0; i < result.length; i += 2) {
                    ranking.push({
                        iata: result[i],
                        views: parseInt(result[i + 1])
                    });
                }
                res.json(ranking);
            } catch (error) {
                console.error("Error en Ranking:", error);
                res.status(500).json({ error: "No se pudo obtener el ranking de Redis" });
            }
        });
        
        app.get('/airports/nearby', async (req, res) => {
        const { lat, lng, radius } = req.query;
        const client = new MongoClient(mongoUri);
        try {
            
            const nearbyIds = await redisGeo.georadius('airports_geo', lng, lat, radius, 'km');
            console.log("IDs encontrados en Redis:", nearbyIds);
            await client.connect();
            const db = client.db('airport_db');
            const airports = await db.collection('airports').find({
                $or: [
                    { iata_faa: { $in: nearbyIds } },
                    { icao: { $in: nearbyIds } }
                ]
            }).toArray();
            res.json(airports);
        } catch (error) {
            res.status(500).json({ error: error.message });
        } finally {
            await client.close();
        }
        });

        // GET /airports/:code devuelve uno (buscando por IATA o ICAO) y suma popularidad
        app.get('/airports/:code', async (req, res) => {
            const code = req.params.code.toUpperCase();
            const client = new MongoClient(mongoUri);
            
            try {
                await client.connect();
                const db = client.db('airport_db');
                
                // Buscamos si el código coincide con el campo iata O con el campo icao
                const airport = await db.collection('airports').findOne({
                    $or: [
                        { iata_faa: code },
                        { icao: code }
                    ]
                });
                
                if (airport) {
                    // Usamos siempre el IATA para el ranking
                    const rankingKey = airport.iata_faa || code;

                    // Sumar +1 en Redis Popularidad
                    await redisPop.zincrby('airport_popularity', 1, rankingKey);
                    // 1 día (86400 segundos)
                    await redisPop.expire('airport_popularity', 86400);
                    
                    res.json(airport);
                } else {
                    res.status(404).json({ error: "Aeropuerto no encontrado" });
                }
            } catch (error) {
                res.status(500).json({ error: "Error en el servidor" });
            } finally {
                await client.close();
            }
        });
        const PORT = 3000;
        app.listen(PORT, () => {
            console.log(`Servidor corriendo en http://localhost:${PORT}`);
        });

    } catch (error) {
        console.error("Error al iniciar el servidor:", error);
        process.exit(1);
    }
}

startServer();