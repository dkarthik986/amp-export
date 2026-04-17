const HtmlWebpackPlugin      = require("html-webpack-plugin");
const ModuleFederationPlugin = require("webpack/lib/container/ModuleFederationPlugin");
const Dotenv                 = require("dotenv-webpack");
const path                   = require("path");
require("dotenv").config();

const PORT = parseInt(process.env.PORT) || 3003;

module.exports = {
  entry: "./src/index.js",
  output: { filename: "[name].[contenthash].js", path: path.resolve(__dirname, "dist"), publicPath: "auto", clean: true },
  resolve: { extensions: [".js", ".jsx"] },
  module: {
    rules: [
      { test: /\.(js|jsx)$/, exclude: /node_modules/, use: { loader: "babel-loader", options: { presets: ["@babel/preset-env", "@babel/preset-react"], plugins: ["@babel/plugin-transform-runtime"] } } },
      { test: /\.css$/, use: ["style-loader", "css-loader"] },
    ],
  },
  plugins: [
    new Dotenv({ systemvars: true }),
    new ModuleFederationPlugin({
      name: "profile",
      filename: "remoteEntry.js",
      exposes: { "./App": "./src/App" },
      shared: {
        react:              { singleton: true, requiredVersion: "^18.2.0" },
        "react-dom":        { singleton: true, requiredVersion: "^18.2.0" },
        "react-router-dom": { singleton: true, requiredVersion: "^6.21.0" },
        axios:              { singleton: true, requiredVersion: "^1.6.0" },
      },
    }),
    new HtmlWebpackPlugin({ template: "./public/index.html" }),
  ],
devServer: {
  host: "0.0.0.0",
  port: PORT,
  allowedHosts: "all",
  historyApiFallback: true,
  hot: true,
  headers: { "Access-Control-Allow-Origin": "*" },
},
};
