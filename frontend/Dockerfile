FROM node:18-alpine as build

WORKDIR /app

# Копируем package.json
COPY package.json ./

# Устанавливаем ВСЕ зависимости (включая devDependencies)
RUN npm install

# Копируем исходники
COPY . .

# Собираем проект
RUN npm run build

# Production
FROM nginx:alpine
COPY --from=build /app/build /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 3000
CMD ["nginx", "-g", "daemon off;"]