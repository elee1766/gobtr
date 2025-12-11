/* @refresh reload */
import "./main.css";
import { lazy } from "solid-js";
import { render } from "solid-js/web";
import { Router, Route } from "@solidjs/router";
import App from "./App";

// Code-split pages
const Home = lazy(() => import(/* webpackChunkName: "home" */ "./pages/Home"));
const FilesystemDetail = lazy(() => import(/* webpackChunkName: "filesystem" */ "./pages/FilesystemDetail"));
const Settings = lazy(() => import(/* webpackChunkName: "settings" */ "./pages/Settings"));

render(
  () => (
    <Router root={App}>
      <Route path="/" component={Home} />
      <Route path="/settings" component={Settings} />
      <Route path="/fs/:id" component={FilesystemDetail} />
      <Route path="/fs/:id/:tab" component={FilesystemDetail} />
    </Router>
  ),
  document.getElementById("root")!
);
