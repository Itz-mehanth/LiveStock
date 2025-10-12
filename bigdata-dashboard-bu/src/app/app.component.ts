import { Component } from '@angular/core';
import { DashboardComponent } from '../../../bigdata-dashboard/src/app/dashboard/dashboard';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [DashboardComponent],
  template: `<app-dashboard></app-dashboard>`
})
export class AppComponent {}
