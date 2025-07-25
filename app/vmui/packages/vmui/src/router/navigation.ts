import router, { routerOptions } from "./index";

export enum NavigationItemType {
  internalLink,
  externalLink,
}

export interface NavigationItem {
  label?: string,
  value?: string,
  hide?: boolean
  submenu?: NavigationItem[],
  type?: NavigationItemType,
}

/**
 * Submenu for Alerts tab
 */

const getAlertsNav = () => [
  { value: router.rules },
  { value: router.alerts },
  { value: router.notifiers },
];

interface NavigationConfig {
  showAlertLink: boolean,
}

/**
 * VictoriaLogs navigation menu
 */
export const getLogsNavigation = ({
  showAlertLink,
}: NavigationConfig): NavigationItem[] => [
  {
    label: routerOptions[router.home].title,
    value: router.home,
  },
  {
    value: "Alerts",
    submenu: getAlertsNav(),
    hide: !showAlertLink,
  },
];
