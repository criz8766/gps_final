# Etapa 1: Construir los archivos estáticos de React
FROM node:18-alpine as build
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . ./
RUN npm run build

# Etapa 2: Servir los archivos estáticos con Nginx
FROM nginx:1.25-alpine
COPY --from=build /app/build /usr/share/nginx/html
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
