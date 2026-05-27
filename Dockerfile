FROM node:18-alpine
WORKDIR /usr/src/app
COPY package*.json ./
RUN npm install

# Copiar assets de npm a la carpeta static/vendor para servirlos offline
RUN mkdir -p static/vendor && \
    cp node_modules/@phosphor-icons/web/src/index.js static/vendor/phosphor-icons.js && \
    cp node_modules/chart.js/dist/chart.umd.js static/vendor/chart.js

# Descargar fuente Inter localmente (no requiere internet en runtime)
RUN apk add --no-cache curl && \
    curl -sL "https://fonts.gstatic.com/s/inter/v13/UcCO3FwrK3iLTeHuS_fvQtMwCp50KnMw2boKoduKmMEVuLyfAZ9hiJ-Ek-_EeA.woff2" \
         -o static/vendor/inter.woff2 || true && \
    curl -sL "https://fonts.gstatic.com/s/outfit/v11/QGYyz_MVcBeNP4NjuGObqx1XmO1I4TC1C4G-EiAou6Y.woff2" \
         -o static/vendor/outfit.woff2 || true

COPY . .
EXPOSE 3000
CMD ["node", "server.js"]
