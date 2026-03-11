import { 
  Activity, 
  BarChart3, 
  ChefHat, 
  Database, 
  Globe,
  Newspaper
} from "lucide-react";
import { Link, useLocation } from "wouter";
import {
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
} from "@/components/ui/sidebar";

const mainNavItems = [
  { title: "Dashboard", url: "/", icon: BarChart3 },
  { title: "Analysis Pipeline", url: "/pipeline", icon: Activity },
];

export function AppSidebar() {
  const [location] = useLocation();

  return (
    <Sidebar className="border-r border-border/50 bg-sidebar-background">
      <SidebarHeader className="p-6">
        <div className="flex items-center gap-3">
          <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-primary text-primary-foreground shadow-sm">
            <ChefHat size={22} className="stroke-[1.5]" />
          </div>
          <div className="flex flex-col">
            <span className="font-display font-bold text-lg leading-none tracking-tight">Trendscope</span>
            <span className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold mt-0.5">by Foodoscope</span>
          </div>
        </div>
      </SidebarHeader>
      
      <SidebarContent className="px-4">
        <SidebarGroup>
          <SidebarGroupLabel className="text-xs font-semibold uppercase tracking-wider text-muted-foreground/70 mb-2">
            Overview
          </SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              {mainNavItems.map((item) => {
                const isActive = location === item.url;
                return (
                  <SidebarMenuItem key={item.title}>
                    <SidebarMenuButton 
                      asChild 
                      isActive={isActive}
                      className={`h-11 rounded-lg transition-all duration-200 ${
                        isActive 
                          ? "bg-primary/5 text-primary font-medium shadow-sm" 
                          : "text-sidebar-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground"
                      }`}
                    >
                      <Link href={item.url} className="flex items-center gap-3 px-3">
                        <item.icon size={18} className={isActive ? "stroke-[2]" : "stroke-[1.5]"} />
                        <span>{item.title}</span>
                      </Link>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                );
              })}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>

        <SidebarGroup className="mt-8">
          <SidebarGroupLabel className="text-xs font-semibold uppercase tracking-wider text-muted-foreground/70 mb-2">
            Data Sources
          </SidebarGroupLabel>
          <SidebarGroupContent>
            <div className="space-y-3 px-3">
              <div className="flex items-center justify-between text-sm text-muted-foreground">
                <div className="flex items-center gap-2">
                  <Database size={14} />
                  <span>Reddit Data</span>
                </div>
                <span className="flex h-2 w-2 rounded-full bg-emerald-500"></span>
              </div>
              <div className="flex items-center justify-between text-sm text-muted-foreground">
                <div className="flex items-center gap-2">
                  <Database size={14} />
                  <span>Discord Arch.</span>
                </div>
                <span className="flex h-2 w-2 rounded-full bg-emerald-500"></span>
              </div>
              <div className="flex items-center justify-between text-sm text-muted-foreground">
                <div className="flex items-center gap-2">
                  <Globe size={14} />
                  <span>Google Trends</span>
                </div>
                <span className="flex h-2 w-2 rounded-full bg-emerald-500"></span>
              </div>
              <div className="flex items-center justify-between text-sm text-muted-foreground">
                <div className="flex items-center gap-2">
                  <Newspaper size={14} />
                  <span>GDELT News</span>
                </div>
                <span className="flex h-2 w-2 rounded-full bg-emerald-500"></span>
              </div>
            </div>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>
    </Sidebar>
  );
}
