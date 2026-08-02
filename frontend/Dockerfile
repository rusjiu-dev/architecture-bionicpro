# frontend/Dockerfile

# Этап 1: Сборка
FROM node:18-alpine AS build

WORKDIR /app

# Копируем package.json отдельно для кеширования
COPY package*.json ./
RUN npm install

# Копируем остальной код
COPY . .
RUN npm run build

# Этап 2: Production
FROM nginx:alpine

# Копируем собранные файлы
COPY --from=build /app/build /usr/share/nginx/html

# Копируем конфигурацию nginx (если есть)
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]