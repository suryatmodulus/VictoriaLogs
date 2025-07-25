import { FC, useState } from "preact/compat";
import { HashRouter, Route, Routes } from "react-router-dom";
import AppContextProvider from "./contexts/AppContextProvider";
import ThemeProvider from "./components/Main/ThemeProvider/ThemeProvider";
import ExploreLogs from "./pages/ExploreLogs/ExploreLogs";
import LogsLayout from "./layouts/LogsLayout/LogsLayout";
import ExploreRules from "./pages/ExploreAlerts/ExploreRules";
import ExploreNotifiers from "./pages/ExploreAlerts/ExploreNotifiers";
import "./constants/markedPlugins";
import router from "./router";

const App: FC = () => {
  const [loadedTheme, setLoadedTheme] = useState(false);

  return <>
    <HashRouter>
      <AppContextProvider>
        <>
          <ThemeProvider onLoaded={setLoadedTheme}/>
          {loadedTheme && (
            <Routes>
              <Route
                path={"/"}
                element={<LogsLayout/>}
              >
                <Route
                  path={"/"}
                  element={<ExploreLogs/>}
                />
                <Route
                  path={router.rules}
                  element={<ExploreRules
                    ruleTypeFilter=""
                  />}
                />
                <Route
                  path={router.alerts}
                  element={<ExploreRules
                    ruleTypeFilter="alert"
                  />}
                />
                <Route
                  path={router.notifiers}
                  element={<ExploreNotifiers/>}
                />
              </Route>
            </Routes>
          )}
        </>
      </AppContextProvider>
    </HashRouter>
  </>;
};

export default App;
