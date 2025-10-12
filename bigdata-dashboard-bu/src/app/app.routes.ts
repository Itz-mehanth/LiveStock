import { Routes } from '@angular/router';
import { DashboardComponent } from '../../../bigdata-dashboard/src/app/dashboard/dashboard';

export const routes: Routes = [
  // Redirect empty path to '/dashboard'
  { path: '', redirectTo: '/dashboard', pathMatch: 'full' },
  
  // Define the dashboard route
  { path: 'dashboard', component: DashboardComponent }
];