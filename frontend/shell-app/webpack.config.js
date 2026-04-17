const HtmlWebpackPlugin       = require("html-webpack-plugin");
const ModuleFederationPlugin  = require("webpack/lib/container/ModuleFederationPlugin");
const Dotenv                  = require("dotenv-webpack");
const path                    = require("path");
require("dotenv").config();

// All URLs read from .env — no hardcoding
const USER_MFE_URL   = process.env.USER_MFE_URL   || "http://localhost:3001";
const SEARCH_MFE_URL = process.env.SEARCH_MFE_URL || "http://localhost:3002";
const PROFILE_MFE_URL= process.env.PROFILE_MFE_URL|| "http://localhost:3003";
const PORT           = parseInt(process.env.PORT)  || 3000;

module.exports = {
  entry: "./src/index.js",
  output: {
    filename: "[name].[contenthash].js",
    path: path.resolve(__dirname, "dist"),
    publicPath: "auto",
    clean: true,
  },
  resolve: { extensions: [".js", ".jsx"] },
  module: {
    rules: [
      {
        test: /\.(js|jsx)$/,
        exclude: /node_modules/,
        use: {
          loader: "babel-loader",
          options: {
            presets: ["@babel/preset-env", "@babel/preset-react"],
            plugins: ["@babel/plugin-transform-runtime"],
          },
        },
      },
      { test: /\.css$/, use: ["style-loader", "css-loader"] },
      {
        test: /\.(svg|png|jpg|jpeg|webp|avif|gif|bmp|tiff|ico)$/i,
        type: "asset/resource",
        generator: { filename: "assets/[hash][ext]" },
      },
    ],
  },
  plugins: [
    new Dotenv({ systemvars: true }),
    new ModuleFederationPlugin({
      name: "shell",
      remotes: {
        user_management: `user_management@${USER_MFE_URL}/remoteEntry.js`,
        search:          `search@${SEARCH_MFE_URL}/remoteEntry.js`,
        profile:         `profile@${PROFILE_MFE_URL}/remoteEntry.js`,
      },
      shared: {
        react:              { singleton: true, requiredVersion: "^18.2.0", eager: true },
        "react-dom":        { singleton: true, requiredVersion: "^18.2.0", eager: true },
        "react-router-dom": { singleton: true, requiredVersion: "^6.21.0", eager: true },
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