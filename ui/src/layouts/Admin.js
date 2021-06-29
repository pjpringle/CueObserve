import React, { useContext } from "react";
import { Switch, Route, Redirect } from "react-router-dom";
import ReactNotification from 'react-notifications-component';

// components

import AdminNavbar from "components/Navbars/AdminNavbar.js";
import Sidebar from "components/Sidebar/Sidebar.js";
import HeaderStats from "components/Headers/HeaderStats.js";

// views
import Datasets from "views/admin/Datasets";
import Dataset from "views/admin/Dataset";
import Connections from "views/admin/Connections";
import AnomalyDefTable from "views/admin/AnomalyDefTable";

// contexts
import { GlobalContextProvider } from "./GlobalContext";

export default function Admin() {
  return (
    <>
      <GlobalContextProvider>
        <Sidebar />
        <ReactNotification />
        <div className="relative md:ml-64 bg-gray-200">
          <AdminNavbar />
          {/* Header */}
          <HeaderStats />
          <div className="px-0 md:px-0 mx-auto w-full" style={{minHeight: "calc(100vh - 0px)", padding: "1rem 0rem 0 0rem"}}>
            <Switch>
              <Route path="/anomalys" exact component={Anomalys} />
              <Route path="/connections" exact component={Connections} />
              <Route path="/anomalyDefinitions" exact component={AnomalyDefTable} />

              <Redirect from="/" to="/datasets" />
            </Switch>
          </div>
        </div>
      </GlobalContextProvider>
    </>
  );
}
