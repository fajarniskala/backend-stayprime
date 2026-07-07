const { Kafka, Partitioners } = require('kafkajs');
require('dotenv').config();

// Inisialisasi client Kafka (Tetap sama)
const kafka = new Kafka({
    clientId: process.env.KAFKA_CLIENT_ID || 'stayprime-client',
    // DAFTARKAN SEMUA BROKER DI SINI
    brokers: ['kafka-1:9092', 'kafka-2:9093', 'kafka-3:9094'], 
    logLevel: 1
});

const producer = kafka.producer({
    createPartitioner: Partitioners.LegacyPartitioner
});

// Variabel penanda biar kita tau producer udah nyambung atau belum
let isProducerConnected = false;

async function initKafka() {
    try {
        const admin = kafka.admin();
        await admin.connect();
        
        // Daftar 5 topik utama sistem StayPrime
        const topicsToCreate = [
            { topic: 'reservasi' },
            { topic: 'check_in' },
            { topic: 'layanan' },
            { topic: 'checkout' },
            { topic: 'pembayaran' }
        ];

        // Membuat topik jika belum ada
        await admin.createTopics({
            topics: topicsToCreate,
            waitForLeaders: true,
            // TAMBAHKAN INI:
            replicationFactor: 3, 
            numPartitions: 3
        });
        
        console.log('✅ [Kafka] Topik berhasil disinkronisasi.');
        await admin.disconnect();
        
        // Hubungkan producer untuk pertama kali
        await producer.connect();
        isProducerConnected = true;
        console.log('✅ [Kafka] Producer siap mengirim pesan.');

        // FITUR SAKTI: Event Listener jika producer terputus (Disconnected)
        producer.on(producer.events.DISCONNECT, async () => {
            console.warn('⚠️ [Kafka] Producer terputus! Mencoba menghubungkan kembali...');
            isProducerConnected = false;
        });

    } catch (error) {
        console.error('❌ [Kafka Error] Gagal inisialisasi:', error.message);
    }
}

// Fungsi sendMessage yang sudah di-Upgrade dengan Auto-Reconnect
async function sendMessage(topic, messageObj) {
    try {
        // CEK DULU: Kalau keputus gara-gara ditabrak Consumer, hubungin lagi otomatis!
        if (!isProducerConnected) {
            console.log('🔄 [Kafka] Menghubungkan ulang Producer sebelum mengirim pesan...');
            await producer.connect();
            isProducerConnected = true;
        }

        await producer.send({
            topic: topic,
            messages: [{ value: JSON.stringify(messageObj) }],
        });
        console.log(`📨 [Kafka] Pesan berhasil dikirim ke topik '${topic}'`);
    } catch (error) {
        console.error(`❌ [Kafka Producer] Gagal kirim ke ${topic}:`, error.message);
        throw error; // Lempar error ke API agar UI dikasih tau
    }
}

module.exports = { kafka, producer, initKafka, sendMessage };