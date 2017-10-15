FROM node:latest
ENV NODE_ENV production
RUN mkdir /app
WORKDIR /app
COPY index.js package.json package-lock.json /app/
RUN npm install
CMD node index.js
