FROM apify/actor-node:22

COPY package*.json ./

RUN npm --quiet set progress=false \
    && npm install --omit=dev --omit=optional \
    && echo "Installed NPM packages:" \
    && (npm list --omit=dev --all || true) \
    && echo "Node.js version:" && node --version \
    && echo "NPM version:" && npm --version

COPY . ./

# Invoke the interpreter directly (not `npm start`) so the runtime's
# debug mode can attach to the Actor process itself.
CMD ["node", "src/main.js"]
