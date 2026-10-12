# Uses only the Apify SDK preinstalled in the base image: no npm install, no network during the build.
FROM apify/actor-node:24
COPY --chown=myuser:myuser src ./src
CMD ["node", "src/main.js"]
