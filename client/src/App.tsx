import { Switch, Route } from "wouter";
import { queryClient } from "./lib/queryClient";
import { QueryClientProvider } from "@tanstack/react-query";
import { Toaster } from "@/components/ui/toaster";
import { TooltipProvider } from "@/components/ui/tooltip";
import { SidebarProvider } from "@/components/ui/sidebar";
import { AppSidebar } from "@/components/app-sidebar";

import Dashboard from "@/pages/dashboard";
import Pipeline from "@/pages/pipeline";
import NotFound from "@/pages/not-found";

function Router() {
  return (
    <Switch>
      <Route path="/" component={Dashboard}/>
      <Route path="/pipeline" component={Pipeline}/>
      <Route component={NotFound} />
    </Switch>
  );
}

function App() {
  const sidebarStyle = {
    "--sidebar-width": "18rem",
    "--sidebar-width-icon": "4rem",
  } as React.CSSProperties;

  return (
    <QueryClientProvider client={queryClient}>
      <TooltipProvider>
        <SidebarProvider style={sidebarStyle}>
          <div className="flex min-h-screen w-full bg-background overflow-hidden selection:bg-primary/10 selection:text-primary">
            <AppSidebar />
            <main className="flex-1 w-full relative h-screen overflow-y-auto overflow-x-hidden">
              <Router />
            </main>
          </div>
        </SidebarProvider>
        <Toaster />
      </TooltipProvider>
    </QueryClientProvider>
  );
}

export default App;
