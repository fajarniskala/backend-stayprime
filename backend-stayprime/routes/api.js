const express = require('express');
const router = express.Router();
const { pool } = require('../config/db'); 
const { sendMessage } = require('../config/kafka'); 
const { initMongo } = require('../config/mongo'); 

router.post('/register', async (req, res) => {
    const { nik, namaTamu, kontak } = req.body;
    try {
        // CEK DUPLIKAT: Cari apakah NIK KTP ini udah pernah nginep
        const [existingGuest] = await pool.query("SELECT * FROM guests WHERE NIK = ?", [nik]);
        
        if (existingGuest.length > 0) {
            // Kalau udah ada, tolak pendaftaran dan suruh langsung reservasi
            return res.status(400).json({ error: `NIK ${nik} sudah terdaftar atas nama ${existingGuest[0].Nama_Tamu}! Silakan langsung pilih di menu dropdown Reservasi.` });
        }

        // Kalau aman (belum ada), daftarkan ke database
        await pool.query("INSERT INTO guests (NIK, Nama_Tamu, Kontak) VALUES (?, ?, ?)", [nik, namaTamu, kontak]);
        res.status(200).json({ success: true, message: "Identitas tamu baru berhasil didaftarkan!" });
        
    } catch (err) { 
        console.error("Error Registrasi Tamu:", err);
        res.status(500).json({ error: "Gagal mendaftarkan tamu. Pastikan NIK berisi 16 digit angka." }); 
    }
});

router.post('/pelunasan_checkin', async (req, res) => {
    const { resId, sisaBayar } = req.body;
    const sisa = Number(sisaBayar) || 0;
    try {
        if(sisa > 0) {
            await pool.query("UPDATE invoices SET Dibayar = Total, Status = 'PAID', Waktu_Bayar = NOW() WHERE ResID = ? AND Tipe_Transaksi = 'KAMAR'", [resId]);
            await sendMessage('pembayaran', { resId, amount: sisa, action: 'LUNAS_KAMAR', timestamp: new Date() });
        } else {
            await pool.query("UPDATE invoices SET Status = 'PAID', Waktu_Bayar = NOW() WHERE ResID = ? AND Tipe_Transaksi = 'KAMAR' AND Status != 'PAID'", [resId]);
        }

        await pool.query("UPDATE reservations SET Status = 'CHECKED_IN', Waktu_CheckIn = NOW() WHERE ResID = ?", [resId]);
        const [rows] = await pool.query("SELECT RoomID FROM reservations WHERE ResID = ?", [resId]);
        if (rows.length > 0) await pool.query("UPDATE rooms SET Status = 'OCCUPIED' WHERE RoomID = ?", [rows[0].RoomID]);
        
        await sendMessage('check_in', { resId, action: 'CHECK_IN_SUCCESS', timestamp: new Date() });
        res.status(200).json({ success: true, message: "Kamar lunas dan tamu berhasil Check-In!" });
    } catch (err) { 
        console.error("API Pelunasan Error:", err);
        res.status(500).json({ error: "Gagal memproses Check-In & Pelunasan" }); 
    }
});

router.post('/checkout', async (req, res) => {
    const { resId } = req.body;
    try {
        const [unpaidSrv] = await pool.query("SELECT * FROM invoices WHERE ResID = ? AND Tipe_Transaksi = 'LAYANAN' AND Status = 'UNPAID'", [resId]);
        if (unpaidSrv.length > 0) return res.status(400).json({ error: "Gagal Check-Out! Ada layanan F&B yang belum lunas." });

        const [unpaidKmr] = await pool.query("SELECT * FROM invoices WHERE ResID = ? AND Tipe_Transaksi = 'KAMAR' AND Status = 'UNPAID'", [resId]);
        if (unpaidKmr.length > 0) return res.status(400).json({ error: "Gagal Check-Out! Tagihan perpanjangan/upgrade kamar belum dilunasi." });

        await pool.query("UPDATE reservations SET Status = 'CHECKED_OUT', Waktu_CheckOut = NOW() WHERE ResID = ?", [resId]);
        const [rows] = await pool.query("SELECT RoomID FROM reservations WHERE ResID = ?", [resId]);
        if (rows.length > 0) await pool.query("UPDATE rooms SET Status = 'DIRTY' WHERE RoomID = ?", [rows[0].RoomID]);
        await sendMessage('checkout', { resId, action: 'CHECK_OUT_SUCCESS', timestamp: new Date() });
        res.status(200).json({ success: true, message: "Berhasil Check-Out" });
    } catch (err) { res.status(500).json({ error: "Gagal Check-Out" }); }
});

router.post('/selesai_bersih', async (req, res) => {
    const { roomId, resId } = req.body; // Sekarang menerima roomId langsung dari frontend
    try {
        await pool.query("UPDATE rooms SET Status = 'READY' WHERE RoomID = ?", [roomId]);
        
        // Tetap kirim ke Kafka buat log audit
        await sendMessage('layanan', { resId: resId || '-', roomId, action: 'CLEANING_COMPLETED', timestamp: new Date() });
        
        res.status(200).json({ success: true, message: "Kamar siap digunakan kembali!" });
    } catch (err) { 
        res.status(500).json({ error: "Gagal update kamar" }); 
    }
});

// LOGIKA RESERVASI DENGAN SISTEM ANTI-DATA HANTU
router.post('/reservasi', async (req, res) => {
    const { resId, guestId, roomId, tglMasuk, tglKeluar, hargaKamar, dpAmount } = req.body;
    const total = Number(hargaKamar) || 0;
    const dp = Number(dpAmount) || 0;

    try {
        // 1. Masukkan Reservasi
        await pool.query(
            "INSERT INTO reservations (ResID, GuestID, RoomID, Tgl_Masuk, Tgl_Keluar, Harga, Status) VALUES (?, ?, ?, ?, ?, ?, 'RESERVED')",
            [resId, guestId, roomId, tglMasuk, tglKeluar, total]
        );
        
        let statusTagihan = (dp >= total && total > 0) ? 'PAID' : 'UNPAID';
        
        // 2. Masukkan Invoice
        try {
            await pool.query(
                "INSERT INTO invoices (ResID, Tipe_Transaksi, Deskripsi, Total, Dibayar, Status, Waktu_Dibuat) VALUES (?, 'KAMAR', 'Sewa Kamar', ?, ?, ?, NOW())",
                [resId, total, dp, statusTagihan]
            );
        } catch (invoiceErr) {
            // JIKA INVOICE GAGAL MASUK, HAPUS RESERVASI BIAR GAK JADI HANTU
            await pool.query("DELETE FROM reservations WHERE ResID = ?", [resId]);
            throw invoiceErr; 
        }

        await sendMessage('reservasi', { resId, roomId, action: 'NEW_RESERVATION', dpAmount: dp, timestamp: new Date() });
        res.status(200).json({ success: true, message: "Reservasi dan DP berhasil dicatat!" });
    } catch (err) { 
        console.error("API Reservasi Error:", err);
        res.status(500).json({ error: "Gagal membuat reservasi. " + err.message }); 
    }
});

router.post('/perpanjang_kamar', async (req, res) => {
    const { resId, tglBaru, tambahanHarga } = req.body;
    const nambah = Number(tambahanHarga) || 0;
    try {
        await pool.query("UPDATE reservations SET Tgl_Keluar = ?, Harga = Harga + ? WHERE ResID = ?", [tglBaru, nambah, resId]);
        await pool.query("UPDATE invoices SET Total = Total + ?, Status = 'UNPAID' WHERE ResID = ? AND Tipe_Transaksi = 'KAMAR'", [nambah, resId]);
        
        await sendMessage('reservasi', { resId, action: 'EXTEND_STAY', tambahanHarga: nambah, tglBaru, timestamp: new Date() });
        res.status(200).json({ success: true, message: "Perpanjangan hari berhasil! Tagihan ditambahkan ke Invoice." });
    } catch (err) { res.status(500).json({ error: "Gagal memproses perpanjangan kamar" }); }
});

router.post('/pindah_kamar', async (req, res) => {
    const { resId, oldRoomId, newRoomId, tambahanHarga } = req.body;
    const nambah = Number(tambahanHarga) || 0;
    try {
        await pool.query("UPDATE rooms SET Status = 'DIRTY' WHERE RoomID = ?", [oldRoomId]);
        await pool.query("UPDATE rooms SET Status = 'OCCUPIED' WHERE RoomID = ?", [newRoomId]);
        await pool.query("UPDATE reservations SET RoomID = ?, Harga = Harga + ? WHERE ResID = ?", [newRoomId, nambah, resId]);

        if (nambah > 0) {
            await pool.query(
                "INSERT INTO invoices (ResID, Tipe_Transaksi, Deskripsi, Total, Dibayar, Status, Waktu_Dibuat) VALUES (?, 'KAMAR', 'Upgrade Kamar', ?, 0, 'UNPAID', NOW())",
                [resId, nambah]
            );
        }
        await sendMessage('reservasi', { resId, action: 'TRANSFER_ROOM', oldRoomId, newRoomId, tambahanHarga: nambah, timestamp: new Date() });
        res.status(200).json({ success: true, message: "Pindah kamar berhasil diproses!" });
    } catch (err) { res.status(500).json({ error: "Gagal memproses pindah kamar." }); }
});

router.post('/layanan', async (req, res) => {
    const { resId, namaLayanan, harga, staffId } = req.body;
    try {
        const [result] = await pool.query("INSERT INTO services (ResID, Nama_Layanan, Harga, Status, Staf_ID, Waktu_Pesan) VALUES (?, ?, ?, 'PENDING', ?, NOW())", [resId, namaLayanan, harga, staffId]);
        await pool.query("INSERT INTO invoices (ResID, ServiceID, Tipe_Transaksi, Deskripsi, Total, Dibayar, Status, Waktu_Dibuat) VALUES (?, ?, 'LAYANAN', ?, ?, 0, 'UNPAID', NOW())", [resId, result.insertId, namaLayanan, harga]);
        await sendMessage('layanan', { resId, namaLayanan, action: 'NEW_SERVICE_ORDER', timestamp: new Date() });
        res.status(200).json({ success: true, message: "Pesanan layanan ditambahkan!" });
    } catch (err) { res.status(500).json({ error: "Gagal membuat pesanan" }); }
});

router.post('/pembayaran', async (req, res) => {
    const { resId, serviceId, tipeTransaksi, deskripsi, amount } = req.body;
    try {
        if (tipeTransaksi === 'KAMAR') {
            await pool.query("UPDATE invoices SET Dibayar = Total, Status = 'PAID', Waktu_Bayar = NOW() WHERE ResID = ? AND Tipe_Transaksi = 'KAMAR'", [resId]);
        } else {
            await pool.query("UPDATE invoices SET Dibayar = Total, Status = 'PAID', Waktu_Bayar = NOW() WHERE ServiceID = ?", [serviceId]);
        }
        await sendMessage('pembayaran', { resId, tipeTransaksi, amount, action: 'PAYMENT_SUCCESS', timestamp: new Date() });
        res.status(200).json({ success: true, message: `Pembayaran lunas!` });
    } catch (err) { res.status(500).json({ error: "Gagal memproses pembayaran" }); }
});

router.get('/logs', async (req, res) => {
    try {
        const db = await initMongo();
        if (!db) return res.status(500).json({ error: "Koneksi MongoDB terputus" });
        const logs = await db.collection('audit_logs').find().sort({ waktu_eksekusi: -1 }).limit(20).toArray();
        res.status(200).json(logs);
    } catch (err) { res.status(500).json({ error: "Gagal menarik data log" }); }
});

// ROUTE BARU: Untuk menyelesaikan pesanan layanan biar hilang dari layar
router.post('/pesanan_selesai', async (req, res) => {
    const { serviceId } = req.body;
    try {
        // Update status di tabel services jadi COMPLETED
        await pool.query("UPDATE services SET Status = 'COMPLETED' WHERE ServiceID = ?", [serviceId]);
        
        // Kirim jejak log ke Kafka
        await sendMessage('layanan', { serviceId, action: 'SERVICE_COMPLETED', timestamp: new Date() });
        
        res.status(200).json({ success: true, message: "Pesanan layanan berhasil diselesaikan!" });
    } catch (err) { 
        console.error("API Pesanan Selesai Error:", err);
        res.status(500).json({ error: "Gagal menyelesaikan pesanan layanan." }); 
    }
});


// ROUTE SUPER: Cek NIK, Daftar Tamu (Jika Baru), dan Reservasi dalam 1 klik!
router.post('/reservasi_smart', async (req, res) => {
    const { resId, nik, namaTamu, kontak, roomId, tglMasuk, tglKeluar, hargaKamar, dpAmount } = req.body;
    
    // BENTENG PERTAHANAN: Tolak mentah-mentah kalau datanya kosong/nyangkut!
    if (!nik || !namaTamu || !roomId || !tglMasuk) {
        return res.status(400).json({ error: "Sistem mendeteksi ada data yang kosong! Pastikan NIK dan Kamar terisi." });
    }

    const total = Number(hargaKamar) || 0;
    const dp = Number(dpAmount) || 0;

    let connection;
    try {
        connection = await pool.getConnection();
        await connection.beginTransaction(); 

        // 1. CEK ATAU BUAT TAMU BERDASARKAN NIK
        let guestId = null;
        const [existingGuest] = await connection.query("SELECT GuestID FROM guests WHERE NIK = ?", [nik]);
        
        if (existingGuest.length > 0) {
            guestId = existingGuest[0].GuestID; 
        } else {
            const [newGuest] = await connection.query("INSERT INTO guests (NIK, Nama_Tamu, Kontak) VALUES (?, ?, ?)", [nik, namaTamu, kontak]);
            guestId = newGuest.insertId;
        }

        // 2. BUAT RESERVASI
        await connection.query(
            "INSERT INTO reservations (ResID, GuestID, RoomID, Tgl_Masuk, Tgl_Keluar, Harga, Status) VALUES (?, ?, ?, ?, ?, ?, 'RESERVED')",
            [resId, guestId, roomId, tglMasuk, tglKeluar, total]
        );
        
        let statusTagihan = (dp >= total && total > 0) ? 'PAID' : 'UNPAID';
        
        // 3. BUAT INVOICE
        await connection.query(
            "INSERT INTO invoices (ResID, Tipe_Transaksi, Deskripsi, Total, Dibayar, Status, Waktu_Dibuat) VALUES (?, 'KAMAR', 'Sewa Kamar', ?, ?, ?, NOW())",
            [resId, total, dp, statusTagihan]
        );

        await connection.commit(); 
        connection.release();

        // 4. KIRIM LAPORAN KE KAFKA
        await sendMessage('reservasi', { resId, roomId, nik, action: 'SMART_RESERVATION', dpAmount: dp, timestamp: new Date() });
        
        res.status(200).json({ success: true, message: "Verifikasi NIK dan Reservasi sukses!" });
    } catch (err) { 
        if(connection) {
            await connection.rollback(); 
            connection.release();
        }
        res.status(500).json({ error: "Gagal memproses ke Database: " + err.message }); 
    }
});

module.exports = router;