FROM apify/actor-node:22

COPY --chown=myuser:myuser package*.json ./
RUN npm --quiet set progress=false \
    && npm install --omit=dev --omit=optional \
    && rm -r ~/.npm

COPY --chown=myuser:myuser . ./

CMD ["node", "src/main.js"]
