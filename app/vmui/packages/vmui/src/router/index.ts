const router = {
  home: "/",
  icons: "/icons",
  alerts: "/alerts",
  rules: "/groups",
  notifiers: "/notifiers"
};

export interface RouterOptionsHeader {
  tenant?: boolean,
  stepControl?: boolean,
  timeSelector?: boolean,
  executionControls?: boolean,
  globalSettings?: boolean,
  cardinalityDatePicker?: boolean
}

export interface RouterOptions {
  title?: string,
  header: RouterOptionsHeader
}

export const routerOptions: { [key: string]: RouterOptions } = {
  [router.home]: {
    title: "Logs Explorer",
    header: {}
  },
  [router.icons]: {
    title: "Icons",
    header: {}
  },
  [router.alerts]: {
    title: "Alerts",
    header: {}
  },
  [router.rules]: {
    title: "Rules",
    header: {}
  },
  [router.notifiers]: {
    title: "Notifiers",
    header: {}
  },
};

export default router;
