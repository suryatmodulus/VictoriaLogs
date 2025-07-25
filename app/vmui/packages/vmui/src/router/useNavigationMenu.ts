import { processNavigationItems } from "./utils";
import { getLogsNavigation } from "./navigation";
import { useAppState } from "../state/common/StateContext";

const useNavigationMenu = () => {
  const { flags } = useAppState();
  const showAlertLink = Boolean(flags["vmalert.proxyURL"]);
  const menu = getLogsNavigation({ showAlertLink });
  return processNavigationItems(menu);
};

export default useNavigationMenu;
