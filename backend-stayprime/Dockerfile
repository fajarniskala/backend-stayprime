# KITA UPGRADE KE NODE VERSI 20 BIAR MONGOOSE NGGAK CRASH
FROM node:20-alpine
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
# BERUBAH: Menjalankan mode Live Reload (Nodemon)
CMD ["npm", "run", "dev"]