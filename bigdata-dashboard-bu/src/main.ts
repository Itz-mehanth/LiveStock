import { bootstrapApplication } from '@angular/platform-browser';
import { appConfig } from './app/app.config';
import { DashboardComponent } from './app/dashboard/dashboard';

bootstrapApplication(DashboardComponent, appConfig)
  .catch((err) => console.error(err));
